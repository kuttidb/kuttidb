#ifndef KUTTIDB_MANAGED_LAUNCHER_H
#define KUTTIDB_MANAGED_LAUNCHER_H

/* Returns -1 when argv is a normal server invocation; otherwise returns the
 * completed ensure command's process exit status. */
int managed_launcher_maybe_run(int argc, char **argv);

#endif
