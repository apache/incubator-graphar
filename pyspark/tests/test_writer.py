(adj_list_type: AdjListType) -> FileType

Get the adj list topology chunk file type of adj list type.

WARNING! Exceptions from the JVM are not checked inside, it is just a proxy-method!

:param adj_list_type: the input adj list type.
:returns: file format type in gar of the adj list type, if edge info not support the adj list type,
raise an IllegalArgumentException error.
