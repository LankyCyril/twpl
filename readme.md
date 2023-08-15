### Two-phase locking with lockfiles

- Initial release
- Currently supports Linux/POSIX only
- Documentation coming soon

Relies on modifying the number of open file descriptors for a lockfile.

Currently implements the 2PL lock compatibility table:

Lock type  | read-lock | write-lock
-----------|-----------|-----------
read-lock  |     Y     |     N
write-lock |     N     |     N

Read locks are called "concurrent," and write locks are called "exclusive."

The API, the naming, and the implementation are all subjects to change until
the first _stable_ release (i.e. until version 1.0.0, or earlier if explicitly
stated).
