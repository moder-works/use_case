## 0.0.1

* Initial Release (Beta)

## 0.0.1+1

* Expose Flush

## 0.0.1+2

* Expose UseCaseSubscription

# 0.0.1+3

* Allow covariant dynamic args

# 0.0.1+4

* Bug Fixes

# 0.0.1+5

* Print more verbose to console on error

# 0.0.1+6

* No longer print on error, up to user to print as desired

# 0.0.1+7

* Added debug flag for logging

# 0.0.1+9

* Added use case dispose method

# 0.0.1+10

* Added invoke method

# 0.1.0

* Added invoke method

# 0.1.1

* Expose `isExecuting` getter (bool)

# 0.1.2

* Allow concurrent UseCase executions

# 1.0.0

* Switch from string ID to generics 

# 1.1.1

* Specify GIVEN and RESULT in type args of the UseCase

# 1.1.4

* Allow void RESULT and improve type checking

# 1.1.7

* Remove Flutter dependency

# 1.1.8

* Add timeout on execution lock

# 1.1.9

* Made isExecuting observable, cleanup code

# 1.1.10

* Using BehaviorSubject in DartChangeNotifier

# 1.1.11

* Fix deps

# 1.1.12

* Fixed issues, added more error logs 

# 1.4.0

* Each use case starts as soon as it is added. It no longer waits for every use case already running to finish, so a slow use case no longer delays unrelated ones. The 60-second timeout on a batch is gone with the batches.
* Two use cases share a run only when they are the same type with equal arguments. Before, a use case added through `invoke` was matched on its arguments alone, so a second use case of a different type with equal (or no) arguments was silently dropped.
* A failing use case reports `error` and nothing after it. Before, observers then received `done` with null data, and `callFuture` could fail to reject.
* `subscribe<T>()` now sees use cases added through `invoke`.
* A finished run is never reused: the next call runs the use case again.
