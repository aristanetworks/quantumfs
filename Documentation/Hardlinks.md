# Hardlinks

POSIX filesystems must support hardlinks, however, they provide some difficulty
with incompletely loaded copy-on-write trees.

Consider the scenario where we have a large workspace with a deep directory
depth. In fact, the workspace is so large that instantiating all the Inodes is
prohibitively expensive. Within this tree some number arbitrary leafs are
hardlinks which are linked together. It is consequently likely that at least one
of the legs of the hardlink will be in an unloaded part of the tree. If another
leg of the hardlink is modified, all legs must see the new data without
searching and/or modifying the other parts of the tree.

In QuantumFS this and related problems are solved by storing a pointer as the
leaf file in the tree. This pointer points to a tree-wide hardlink table which
is reachable from the WorkspaceRoot object.
