import 'dart:async';

import 'package:test/test.dart';
import 'package:use_case/use_case.dart';

/// Records each run, then waits for [gate] (when given) before answering.
class _Probe extends UseCase<Object?, String> {
  _Probe(this.name, this.runs, {this.gate});

  final String name;
  final List<String> runs;
  final Completer<void>? gate;

  @override
  Future<String> execute(Object? args) async {
    runs.add(name);
    if (gate != null) {
      await gate!.future;
    }
    return '$name:$args';
  }
}

class _OtherProbe extends _Probe {
  _OtherProbe(super.name, super.runs);
}

class _ConcurrentProbe extends _Probe {
  _ConcurrentProbe(super.name, super.runs);

  @override
  bool get allowConcurrency => true;
}

class _Failing extends UseCase<Object?, String> {
  @override
  Future<String> execute(Object? args) async => throw StateError('boom');
}

/// Collects the states one observer sees.
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

Future<void> _until(bool Function() done) async {
  for (var i = 0; i < 200 && !done(); i++) {
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
  expect(done(), isTrue, reason: 'condition never became true');
}

void main() {
  late UseCaseManager manager;
  late List<String> runs;

  setUp(() {
    manager = UseCaseManager();
    runs = [];
  });

  tearDown(() => manager.flush());

  group('use cases of different types never share a run', () {
    test('both run when neither has arguments', () async {
      manager.invoke(_Probe('a', runs));
      manager.invoke(_OtherProbe('b', runs));

      await _settle();

      expect(runs, unorderedEquals(['a', 'b']));
    });

    test('both run when their arguments are equal', () async {
      manager.invoke(_Probe('a', runs), args: 'same');
      manager.invoke(_OtherProbe('b', runs), args: 'same');

      await _settle();

      expect(runs, unorderedEquals(['a', 'b']));
    });
  });

  test('a use case added while another runs does not wait for it', () async {
    final slowGate = Completer<void>();
    final fast = _Recorder();

    manager.invoke(_Probe('slow', runs, gate: slowGate), args: 1);
    await _settle();
    expect(runs, ['slow']);

    manager.invoke(_OtherProbe('fast', runs), observer: fast, args: 2);

    // The slow use case is still blocked on its gate.
    await _until(() => fast.states.contains(UseCaseState.done));
    expect(slowGate.isCompleted, isFalse);
    expect(fast.seen.last.data, 'fast:2');

    slowGate.complete();
  });

  test('the same type with equal arguments shares the run in flight', () async {
    final gate = Completer<void>();
    final first = _Recorder();
    final second = _Recorder();

    manager.invoke(_Probe('a', runs, gate: gate), observer: first, args: 'x');
    manager.invoke(_Probe('a', runs, gate: gate), observer: second, args: 'x');
    await _settle();
    gate.complete();
    await _settle();

    expect(runs, ['a']);
    expect(first.seen.last.data, 'a:x');
    expect(second.seen.last.state, UseCaseState.done);
    expect(second.seen.last.data, 'a:x');
  });

  test('a finished run is not reused: the next call runs again', () async {
    manager.invoke(_Probe('a', runs), args: 'x');
    await _settle();
    manager.invoke(_Probe('a', runs), args: 'x');
    await _settle();

    expect(runs, ['a', 'a']);
  });

  test('allowConcurrency runs the same type and arguments twice', () async {
    manager.invoke(_ConcurrentProbe('c', runs), args: 'x');
    manager.invoke(_ConcurrentProbe('c', runs), args: 'x');

    await _settle();

    expect(runs, ['c', 'c']);
  });

  test('a failing use case reports error and nothing after it', () async {
    final observer = _Recorder();

    manager.invoke(_Failing(), observer: observer);
    await _settle();

    expect(observer.states.last, UseCaseState.error);
    expect(observer.states, isNot(contains(UseCaseState.done)));
    expect(observer.seen.last.error, isA<StateError>());
  });

  test('callFuture rejects with the use case error', () async {
    manager.register<_Failing>(() => _Failing());

    await expectLater(
      manager.callFuture<_Failing>(),
      throwsA(isA<StateError>()),
    );
  });

  test('a subscription on the concrete type sees an invoked use case',
      () async {
    final subscriber = _Recorder();
    final sub = manager.subscribe<_OtherProbe>(subscriber);

    manager.invoke(_Probe('ignored', runs), args: 1);
    manager.invoke(_OtherProbe('seen', runs), args: 2);
    await _settle();

    expect(subscriber.states.last, UseCaseState.done);
    expect(subscriber.seen.last.data, 'seen:2');
    expect(subscriber.seen.map((s) => s.type).toSet(), {_OtherProbe});
    sub.dispose();
  });

  test('isExecuting is true while any use case runs, false after', () async {
    final gate = Completer<void>();

    manager.invoke(_Probe('a', runs, gate: gate));
    manager.invoke(_OtherProbe('b', runs));
    expect(manager.isExecuting.value, isTrue);

    await _settle();
    // b has finished; a is still running.
    expect(manager.isExecuting.value, isTrue);

    gate.complete();
    await _settle();
    expect(manager.isExecuting.value, isFalse);
  });
}
