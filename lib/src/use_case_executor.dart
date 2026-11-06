import 'dart:async';
import 'package:synchronized/synchronized.dart';
import 'package:use_case/use_case.dart';

typedef UCLogger = void Function(String message, UCLogLevel logLevel);

/// Runs use cases and reports their [UseCaseStatus] to observers.
///
/// There is one executor per process: every [UseCaseManager] shares it.
/// Use cases added while others run are queued and start together once the
/// running batch has finished. A use case added with equal arguments while
/// an earlier one is queued or running shares that run unless it sets
/// [UseCase.allowConcurrency]; see [add] for how the two are matched.
class UseCaseExecutor {
  final List<_UseCaseWrapper> _queue = [];
  final Map<Type, List<UseCaseSubscription>> _subscriptions = {};
  final Lock _executionLock;
  final Lock _notificationLock;
  final bool debug;
  final UCLogger? logger;

  /// The process-wide executor, created by the first constructor call.
  static UseCaseExecutor? instance;

  /// Whether any use case is queued or running.
  final DartValueNotifier<bool> isExecuting = DartValueNotifier(false);

  UseCaseExecutor._(
    this._notificationLock,
    this._executionLock,
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

  List<_UseCaseWrapper> _getQueue() {
    return _queue.where((e) {
      return e.status.state == UseCaseState.queued;
    }).toList();
  }

  Future<void> _runQueue() async {
    final queue = _getQueue();

    if (queue.isEmpty) {
      isExecuting.value = false;

      return;
    }

    isExecuting.value = true;

    return _executionLock.synchronized(() async {
      logV('Queue Length: ${queue.length}');

      final runs = <Future<void>>[];

      for (var entry in queue) {
        if (entry.status.state != UseCaseState.queued) {
          continue;
        }

        runs.add(_run(entry));
      }

      return Future.wait(runs);
    }).timeout(
      const Duration(seconds: 60),
      onTimeout: () {
        logE('Timeout while executing UseCases');
        cleanQueue();
        isExecuting.value = false;
        return Future.error('UseCaseExecutor: Timeout');
      },
    ).then((v) async {
      cleanQueue();
      return _runQueue();
    }).onError((error, stackTrace) {
      logE('Queue Failed: $error');

      cleanQueue();

      isExecuting.value = false;
    });
  }

  /// Starts one queued [entry] and returns a future that completes when it
  /// has finished and its observers have been told.
  Future<void> _run(_UseCaseWrapper entry) {
    final useCase = entry.useCase;
    final args = entry.args;
    final observers = entry.observers;
    final type = entry.type;

    if (_subscriptions.containsKey(type)) {
      observers.addAll(_subscriptions[type]!.map((e) => e.observer));
    }

    entry.status = entry.status.copyWith(state: UseCaseState.started);
    _notifyObservers(entry.status, observers);

    final run = Future.sync(() async {
      logV('${useCase.runtimeType} Starting Execution');
      try {
        final r = await useCase.execute(args);
        await useCase.dispose();
        logV('${useCase.runtimeType} Disposed');
        return r;
      } catch (_) {
        try {
          await useCase.dispose();
        } catch (e) {
          logE('${useCase.runtimeType} Dispose Failed: $e');
        }

        rethrow;
      }
    }).onError((error, stackTrace) {
      entry.status = entry.status.copyWith(
        state: UseCaseState.error,
        error: error,
        stackTrace: stackTrace,
      );

      logE('${useCase.runtimeType} Completed With Error: $error');

      _notifyObservers(entry.status, observers);

      logV('${useCase.runtimeType} Finished Execution');
    }).then((val) {
      entry.status = entry.status.copyWith(
        state: UseCaseState.done,
        data: val,
      );

      logV('${useCase.runtimeType} Completed Normally');

      _notifyObservers(entry.status, observers);

      logV('${useCase.runtimeType} Finished Execution');
    });

    entry.status = entry.status.copyWith(state: UseCaseState.waiting);
    _notifyObservers(entry.status, observers);

    return run;
  }

  /// Removes the use cases that have finished from the queue.
  void cleanQueue() {
    _queue.removeWhere((entry) {
      if (entry.isFinished) {
        logV('Removed ${entry.useCase.runtimeType} from the queue');
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

  /// Queues [uc] with [args] and reports its status to [observer].
  ///
  /// When a use case added under the same [T] with equal [args] is already
  /// queued or running, [observer] is attached to that run instead. [T] is the
  /// static type argument, not the runtime type: `UseCaseManager.invoke` adds
  /// every use case as a plain [UseCase].
  void add<T extends UseCase>(T uc, UseCaseObserver? observer, [dynamic args]) {
    // Check UseCase exists with matching args.
    final idx = _queue.indexWhere((q) => q.isType<T>(args));

    // If not exists, add to queue.
    if (idx == -1 || uc.allowConcurrency) {
      logV('${uc.runtimeType} added to the queue');
      _queue.add(_UseCaseWrapper<T>(uc, args, observer));
    } else if (observer != null) {
      // Otherwise grab the existing UseCase
      final existing = _queue[idx];

      // If the existing UseCase is done executing, just relay the status.
      if (existing.isFinished) {
        logV('${uc.runtimeType} used result of already completed UseCase');
        observer.onUseCaseUpdate(existing.status);
        return;
      }

      logV(
          '${uc.runtimeType} added as an observer to an already existing UseCase');

      // Otherwise, attach this observer to the existing UseCase
      existing.observers.add(observer);

      // observer.onUseCaseUpdate( existing.status );
    }

    if (!_executionLock.locked) {
      _runQueue();
    }
  }
}

class _UseCaseWrapper<T extends UseCase> {
  _UseCaseWrapper(this.useCase, this.args, UseCaseObserver? observer)
      : type = T,
        status = UseCaseStatus(T, state: UseCaseState.queued) {
    if (observer != null) {
      observers.add(observer);
      observer.onUseCaseUpdate(status);
    }
  }

  final T useCase;
  final dynamic args;
  final Type type;
  UseCaseStatus status;

  final List<UseCaseObserver> observers = [];

  bool get isFinished =>
      status.state == UseCaseState.done || status.state == UseCaseState.error;

  bool isType<X>(dynamic args) => X == T && args == this.args;
}
