import 'dart:async';
import 'package:collection/collection.dart';
import 'package:synchronized/synchronized.dart';
import 'package:use_case/use_case.dart';

typedef UCLogger = void Function(String message, UCLogLevel logLevel);

/// Runs use cases and reports their [UseCaseStatus] to observers.
///
/// There is one executor per process: every [UseCaseManager] shares it.
/// Each use case starts as soon as it is added and runs beside whatever else
/// is in flight. A use case of the same type with equal arguments as one that
/// is still running shares that run unless it sets
/// [UseCase.allowConcurrency]; see [add].
class UseCaseExecutor {
  final List<_UseCaseWrapper> _queue = [];
  final Map<Type, List<UseCaseSubscription>> _subscriptions = {};
  final Lock _notificationLock;
  final bool debug;
  final UCLogger? logger;

  /// The process-wide executor, created by the first constructor call.
  static UseCaseExecutor? instance;

  /// Whether any use case is queued or running.
  final DartValueNotifier<bool> isExecuting = DartValueNotifier(false);

  UseCaseExecutor._(
    this._notificationLock,
    this.debug,
    this.logger,
  );

  /// Returns the process-wide executor. [debug] and [logger] take effect only
  /// on the first call, which creates it.
  factory UseCaseExecutor({
    bool debug = false,
    UCLogger? logger,
  }) {
    return instance ??= UseCaseExecutor._(
      Lock(
        reentrant: true,
      ),
      debug,
      logger,
    );
  }

  /// Sends [message] to [logger], or prints it when there is no logger and
  /// [debug] is set.
  void log(String message, UCLogLevel logLevel) {
    if (logger != null) {
      logger!(message, logLevel);
    } else if (debug) {
      // ignore: avoid_print
      print('UseCaseExecutor: $message');
    }
  }

  void logV(String message) => log(message, UCLogLevel.verbose);
  void logD(String message) => log(message, UCLogLevel.debug);
  void logI(String message) => log(message, UCLogLevel.info);
  void logW(String message) => log(message, UCLogLevel.warning);
  void logE(String message) => log(message, UCLogLevel.error);

  void _notifyObservers(UseCaseStatus status, List<UseCaseObserver> observers) {
    _notificationLock.synchronized(() {
      for (var observer in observers) {
        observer.onUseCaseUpdate(status);
      }
    });
  }

  /// Reports every queued or running use case as an error to its observers,
  /// then clears the queue and every subscription.
  Future<void> flush() async {
    List<_UseCaseWrapper> queueCopy = List.from(_queue);
    _queue.clear();

    for (var entry in queueCopy) {
      entry.status = entry.status.copyWith(state: UseCaseState.error);
      _notifyObservers(entry.status, entry.observers);
    }

    _subscriptions.clear();
  }

  /// Starts one queued [entry] on its own and tells its observers.
  ///
  /// It never waits for an unrelated use case to finish. When it finishes it
  /// leaves the queue, so a later call runs the use case again.
  void _run(_UseCaseWrapper entry) {
    // Flushed before it started.
    if (!_queue.contains(entry)) {
      return;
    }

    final useCase = entry.useCase;
    final args = entry.args;
    final observers = entry.observers;
    final subscribers = _subscriptions[entry.type];

    if (subscribers != null) {
      observers.addAll(subscribers.map((e) => e.observer));
    }

    entry.status = entry.status.copyWith(state: UseCaseState.started);
    _notifyObservers(entry.status, observers);

    Future.sync(() async {
      logV('${entry.type} Starting Execution');
      try {
        final r = await useCase.execute(args);
        await useCase.dispose();
        logV('${entry.type} Disposed');
        return r;
      } catch (_) {
        try {
          await useCase.dispose();
        } catch (e) {
          logE('${entry.type} Dispose Failed: $e');
        }

        rethrow;
      }
    }).then(
      (val) {
        entry.status = entry.status.copyWith(
          state: UseCaseState.done,
          data: val,
        );

        logV('${entry.type} Completed Normally');

        _notifyObservers(entry.status, observers);
      },
      onError: (Object error, StackTrace stackTrace) {
        entry.status = entry.status.copyWith(
          state: UseCaseState.error,
          error: error,
          stackTrace: stackTrace,
        );

        logE('${entry.type} Completed With Error: $error');

        _notifyObservers(entry.status, observers);
      },
    ).whenComplete(() {
      logV('${entry.type} Finished Execution');
      if (_queue.remove(entry)) {
        logV('Removed ${entry.type} from the queue');
      }
      if (_queue.isEmpty) {
        isExecuting.value = false;
      }
    });

    entry.status = entry.status.copyWith(state: UseCaseState.waiting);
    _notifyObservers(entry.status, observers);
  }

  /// Removes the use cases that have finished from the queue.
  ///
  /// A finished use case already leaves the queue on its own; this remains
  /// for callers that used it before.
  void cleanQueue() {
    _queue.removeWhere((entry) {
      if (entry.isFinished) {
        logV('Removed ${entry.type} from the queue');
      }

      return entry.isFinished;
    });
  }

  /// Reports every later run of [T] to [observer] until the subscription is
  /// disposed.
  UseCaseSubscription subscribe<T extends UseCase>(UseCaseObserver observer) {
    var sub = UseCaseSubscription<T>(observer, this);

    (_subscriptions[T] ??= []).add(sub);

    return sub;
  }

  /// Stops reporting to [sub]'s observer.
  void unsubscribe(UseCaseSubscription sub) {
    _subscriptions[sub.type]?.remove(sub);
  }

  /// Whether [sub] is still subscribed.
  bool hasSubscription(UseCaseSubscription sub) {
    return _subscriptions.containsKey(sub.type) &&
        _subscriptions[sub.type]!.contains(sub);
  }

  void broadcast<T extends UseCase>(T useCase, [dynamic args]) {
    assert(() {
      return _subscriptions.keys.contains(T) && _subscriptions[T]!.isNotEmpty;
    }(), 'Requested broadcast for UseCase $T, which had no subscriptions');

    _subscriptions[T]!.map(
      (e) => add(
        useCase,
        UseCaseHandler(
          onUpdate: (_) {},
        ),
        args,
      ),
    );
  }

  /// Starts [uc] with [args] and reports its status to [observer].
  ///
  /// When a use case of the same runtime type with equal [args] is still
  /// queued or running, [observer] is attached to that run instead, unless
  /// [uc] sets [UseCase.allowConcurrency]. Use cases of different types never
  /// share a run, whatever their arguments.
  void add<T extends UseCase>(T uc, UseCaseObserver? observer, [dynamic args]) {
    final existing = uc.allowConcurrency
        ? null
        : _queue.firstWhereOrNull((q) => q.matches(uc, args));

    if (existing == null) {
      final entry = _UseCaseWrapper(uc, args, observer);
      _queue.add(entry);
      isExecuting.value = true;
      logV('${entry.type} added to the queue');
      scheduleMicrotask(() => _run(entry));
      return;
    }

    if (observer != null) {
      logV(
        '${uc.runtimeType} added as an observer to an already existing UseCase',
      );
      existing.observers.add(observer);
    } else {
      logV(
          '${uc.runtimeType} shares the run already in flight with equal args');
    }
  }
}

class _UseCaseWrapper {
  _UseCaseWrapper(this.useCase, this.args, UseCaseObserver? observer)
      : type = useCase.runtimeType,
        status = UseCaseStatus(
          useCase.runtimeType,
          state: UseCaseState.queued,
        ) {
    if (observer != null) {
      observers.add(observer);
      observer.onUseCaseUpdate(status);
    }
  }

  final UseCase useCase;
  final dynamic args;

  /// The use case's runtime type, never the static type it was added under:
  /// `invoke` adds every use case as a plain [UseCase].
  final Type type;
  UseCaseStatus status;

  final List<UseCaseObserver> observers = [];

  bool get isFinished =>
      status.state == UseCaseState.done || status.state == UseCaseState.error;

  bool matches(UseCase other, dynamic otherArgs) =>
      type == other.runtimeType && args == otherArgs;
}
