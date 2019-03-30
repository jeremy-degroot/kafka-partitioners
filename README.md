# StickyRandomPartitioner

Useful when you have incoming data with a lot of skew, but want to enjoy the benfits of processing similar records together.
Records with the same key will go to the same queue for the configured amount of time, then move to a new partition to balance load between consumers.
Duration is best configured for a multiple of your cache TTL or the lifetime of whatever resource you're trying to use.