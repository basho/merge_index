## lock-does-not-exist.eqc

This counter example checks that, after a drop, async
range/lookup/iterator pids don't try to release locks on files that
don't exist anymore.