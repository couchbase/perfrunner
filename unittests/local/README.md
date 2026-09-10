# Local tier — scratch tests, never committed

Everything here except this README is gitignored: the throwaway tests written while developing
or debugging, which verify your own work in the moment but do not describe behaviour worth
keeping. `make test-all` runs them; no CI target references this directory.

If one turns out to describe behaviour that should keep working, promote it to `unittests/extended/` and commit it.
