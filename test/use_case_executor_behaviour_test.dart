import 'dart:async';

import 'package:test/test.dart';
import 'package:use_case/use_case.dart';

/// Pins what the executor does today, so a refactor that changes it fails.

/// Counts its runs and disposals, and answers with its arguments.
class _Echo extends UseCase<Object?, String> {
  _Echo(this.log, {this.gate});

  final List<String> log;
  final Completer<void>? gate;

  @override
  Future<String> execute(Object? args) async {
    log.add('execute');
    if (gate != null) {
      await gate!.future;
    }
    return 'echo:$args';
  }

  @override
  Future<void> dispose() async => log.add('dispose');
}

class _ConcurrentEcho extends _Echo {
  _ConcurrentEcho(super.log);

  @override
  bool get allowConcurrency => true;
}

class _Failing extends UseCase<Object?, String> {
  _Failing(this.log);

  final List<String> log;

  @override
  Future<String> execute(Object? args) async => throw StateError('boom');

  @override
  Future<void> dispose() async => log.add('dispose');
}

class _Recorder implements UseCaseObserver {
  final List<UseCaseStatus> seen = [];

  List<UseCaseState> get states => seen.map((s) => s.state).toList();

  @override
  void onUseCaseUpdate(UseCaseStatus update) => seen.add(update);
}

/// Lets queued microtasks and notification-lock callbacks run.
Future<void> _settle() async {
  for (var i = 0; i < 20; i++) {
    await Future<void>.delayed(Duration.zero);
  }
}

void main() {
  late UseCaseManager manager;
  late List<String> log;

  setUp(() {
    manager = UseCaseManager();
    log = [];
  });

  tearDown(() => manager.flush());

  test('an observer sees queued, started, waiting, then done with the result',
      () async {
    final observer = _Recorder();
    manager.register<_Echo>(() => _Echo(log));

    manager.call<_Echo>(observer: observer, args: 1);
    await _settle();

    expect(observer.states, [
      UseCaseState.queued,
      UseCaseState.started,
      UseCaseState.waiting,
      UseCaseState.done,
    ]);
    expect(observer.seen.last.data, 'echo:1');
    expect(observer.seen.last.type, _Echo);
  });

  test('callFuture completes with the result', () async {
    manager.register<_Echo>(() => _Echo(log));

    expect(await manager.callFuture<_Echo>(7), 'echo:7');
  });

  test('a use case is disposed once after it succeeds', () async {
    manager.register<_Echo>(() => _Echo(log));

    await manager.callFuture<_Echo>();
    await _settle();

    expect(log, ['execute', 'dispose']);
  });

  test('a failing use case reports the error and is disposed once', () async {
    final observer = _Recorder();
    manager.register<_Failing>(() => _Failing(log));

    manager.call<_Failing>(observer: observer);
    await _settle();

    final error = observer.seen.firstWhere(
      (s) => s.state == UseCaseState.error,
    );
    expect(error.error, isA<StateError>());
    expect(error.stackTrace, isNotNull);
    expect(log, ['dispose']);
  });

  test('the same type with equal arguments shares one run', () async {
    final gate = Completer<void>();
    final first = _Recorder();
    final second = _Recorder();
    manager.register<_Echo>(() => _Echo(log, gate: gate));

    manager.call<_Echo>(observer: first, args: 'x');
    manager.call<_Echo>(observer: second, args: 'x');
    await _settle();
    gate.complete();
    await _settle();

    expect(log.where((e) => e == 'execute'), hasLength(1));
    expect(first.seen.last.data, 'echo:x');
    expect(second.seen.last.state, UseCaseState.done);
    expect(second.seen.last.data, 'echo:x');
  });

  test('the same type with different arguments runs twice', () async {
    manager.register<_Echo>(() => _Echo(log));

    final results = await Future.wait([
      manager.callFuture<_Echo>('a'),
      manager.callFuture<_Echo>('b'),
    ]);

    expect(results, ['echo:a', 'echo:b']);
  });

  test('allowConcurrency runs the same type and arguments twice', () async {
    manager.register<_ConcurrentEcho>(() => _ConcurrentEcho(log));

    manager.call<_ConcurrentEcho>(args: 'x');
    manager.call<_ConcurrentEcho>(args: 'x');
    await _settle();

    expect(log.where((e) => e == 'execute'), hasLength(2));
  });

  test('a subscription sees every run of its type', () async {
    final subscriber = _Recorder();
    manager.register<_Echo>(() => _Echo(log));
    final sub = manager.subscribe<_Echo>(subscriber);

    await manager.callFuture<_Echo>(1);
    await manager.callFuture<_Echo>(2);
    await _settle();

    final done = subscriber.seen.where((s) => s.state == UseCaseState.done);
    expect(done.map((s) => s.data), ['echo:1', 'echo:2']);

    sub.dispose();
    expect(sub.disposed, isTrue);
  });

  test('isExecuting is true while a use case runs and false after', () async {
    final gate = Completer<void>();
    manager.register<_Echo>(() => _Echo(log, gate: gate));

    manager.call<_Echo>();
    await _settle();
    expect(manager.isExecuting.value, isTrue);

    gate.complete();
    await _settle();
    expect(manager.isExecuting.value, isFalse);
  });

  test('flush reports a run in flight as an error', () async {
    final observer = _Recorder();
    final gate = Completer<void>();
    manager.register<_Echo>(() => _Echo(log, gate: gate));

    manager.call<_Echo>(observer: observer);
    await _settle();
    await manager.flush();
    await _settle();

    expect(observer.states, contains(UseCaseState.error));
    gate.complete();
  });
}
