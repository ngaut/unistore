package _import

/// todo, SSTImporter has some methods.
type SSTImporter struct {
	dir ImportDir
}

/// todo, ImportDir has some methods.
type ImportDir struct {
	rootDir  string
	tempDir  string
	cloneDir string
}
