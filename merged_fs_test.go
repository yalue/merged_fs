package merged_fs

import (
	"archive/zip"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

func openZip(path string, t *testing.T) fs.FS {
	// We don't bother to close these .zip archives used for testing.
	f, e := os.Open(path)
	if e != nil {
		t.Logf("Failed opening %s: %s\n", path, e)
		t.FailNow()
	}
	stat, e := f.Stat()
	if e != nil {
		t.Logf("Failed to stat file %s: %s\n", path, e)
		t.FailNow()
	}
	toReturn, e := zip.NewReader(f, stat.Size())
	if e != nil {
		t.Logf("Failed to open zip file %s: %s\n", path, e)
		t.FailNow()
	}
	return toReturn
}

func TestMergedFS(t *testing.T) {
	zip1 := openZip("test_data/test_a.zip", t)
	zip2 := openZip("test_data/test_b.zip", t)
	zip3 := openZip("test_data/test_c.zip", t)

	// Merge all three zip files into a single FS, with zip1 being highest
	// priority, and zip3 being lowest priority.
	merged := NewMergedFS(zip1, NewMergedFS(zip2, zip3))
	expectedFiles := []string{
		"test1.txt",
		"test2.txt",
		"test3.txt",
		"b/0.txt",
		"b/1.txt",
		"b",
		"a",
	}

	e := fstest.TestFS(merged, expectedFiles...)
	if e != nil {
		t.Logf("TestFS failed: %s\n", e)
		t.FailNow()
	}

	// Make sure we can't treat a as both a regular file and a directory.
	_, e = merged.Open("a/test4.txt")
	if e == nil {
		t.Logf("Didn't get expected error when opening a/test4.txt. It " +
			"shouldn't be available, since a is a regular file.\n")
		t.FailNow()
	}
	t.Logf("Got expected error when opening a file in an overridden "+
		"directory: %s", e)
	f, e := merged.Open("a")
	if e != nil {
		t.Logf("Failed to open file a: %s\n", e)
		t.FailNow()
	}
	stat, e := f.Stat()
	if e != nil {
		t.Logf("Failed to Stat() file a: %s\n", e)
		t.FailNow()
	}
	if stat.IsDir() {
		t.Logf("Expected file a to be a regular file, not a directory.\n")
		t.Fail()
	}

	// Make sure this is the copy from test_a.zip, which should be two bytes.
	f, e = merged.Open("test1.txt")
	if e != nil {
		t.Logf("Failed opening test1.txt: %s\n", e)
		t.FailNow()
	}
	stat, e = f.Stat()
	if e != nil {
		t.Logf("Failed to Stat() test1.txt: %s\n", e)
		t.FailNow()
	}
	if stat.Size() != 2 {
		t.Logf("Expected test1.txt to be 2 bytes, got %d.\n", stat.Size())
		t.Fail()
	}
	content, e := io.ReadAll(f)
	if e != nil {
		t.Logf("Failed reading test1.txt's content: %s", e)
		t.FailNow()
	}
	t.Logf("Content of test1.txt: %s\n", string(content))
}

func TestReadFileFS(t *testing.T) {
	zip1 := openZip("test_data/test_a.zip", t)
	zip2 := openZip("test_data/test_b.zip", t)
	zip3 := openZip("test_data/test_c.zip", t)

	// Merge all three zip files into a single FS, with zip1 being highest
	// priority, and zip3 being lowest priority.
	var m fs.FS = NewMergedFS(zip1, NewMergedFS(zip2, zip3))
	merged, ok := m.(fs.ReadFileFS)
	if !ok {
		t.Errorf("Expected MergedFS to implement fs.ReadFileFS interface.")
		return
	}

	// Make sure this is the copy from test_a.zip, which should be two bytes.
	f, err := merged.Open("test1.txt")
	if err != nil {
		t.Errorf("Failed opening test1.txt: %s\n", err)
		return
	}
	stat, err := f.Stat()
	if err != nil {
		t.Errorf("Failed to Stat() test1.txt: %s\n", err)
		return
	}
	if stat.Size() != 2 {
		t.Errorf("Expected test1.txt to be 2 bytes, got %d.\n", stat.Size())
		return
	}
	content, e := io.ReadAll(f)
	if e != nil {
		t.Errorf("Failed reading test1.txt's content: %s", e)
		return
	}
	t.Logf("Content of test1.txt: %s\n", string(content))
	b, err := merged.ReadFile("test1.txt")
	if err != nil {
		t.Errorf("Failed reading test1.txt's content from ReadFile: %s", e)
		return
	}
	if string(b) != string(content) {
		t.Errorf("Contents do not match while reading test1.txt")
		return
	}
}

func TestGlob(t *testing.T) {
	zip1 := openZip("test_data/test_a.zip", t)
	zip2 := openZip("test_data/test_b.zip", t)
	zip3 := openZip("test_data/test_c.zip", t)
	dir1 := os.DirFS("test_data")
	merged := MergeMultiple(zip1, zip2, zip3, dir1)

	results, e := fs.Glob(merged, "*.txt")
	if e != nil {
		t.Logf("Error running fs.Glob: %s\n", e)
		t.FailNow()
	}
	if len(results) != 3 {
		t.Logf("Glob *.txt produced %d results, but 3 were expected.\n",
			len(results))
		t.FailNow()
	}
	for i, r := range results {
		t.Logf("   Glob *.txt result %d: %s\n", i+1, r)
	}

	results, e = fs.Glob(merged, "a/*")
	if e != nil {
		t.Logf("Error running fs.Glob for a/*: %s\n", e)
		t.FailNow()
	}
	if len(results) != 0 {
		t.Logf("Glob a/* didn't give 0 results.\n")
		t.Fail()
	}

	results, e = fs.Glob(merged, "b/*")
	if e != nil {
		t.Logf("Error running fs.Glob: %s\n", e)
		t.FailNow()
	}
	if len(results) != 2 {
		t.Logf("Glob b/* produced %d results, but 2 were expected.\n",
			len(results))
		t.FailNow()
	}
	for i, r := range results {
		t.Logf("   Glob b/* result %d: %s\n", i+1, r)
	}
}

func TestPathPrefixCaching(t *testing.T) {
	// This test makes sure that if a regular file is added to FS A, then it
	// will correctly block a directory with the same name in FS B, so long as
	// path prefix caching is disabled.
	zip1 := openZip("test_data/test_a.zip", t)
	zip2 := openZip("test_data/test_b.zip", t)
	zip3 := openZip("test_data/test_c.zip", t)
	merged := NewMergedFS(zip1, NewMergedFS(zip2, zip3))

	// Make the merged zip files be lower priority than the test_data folder,
	// to keep matters simple.
	merged = NewMergedFS(os.DirFS("test_data"), merged)
	expectedFiles := []string{
		"test_a.zip",
		"test_b.zip",
		"test_c.zip",
		"test1.txt",
		"test2.txt",
		"test3.txt",
		"b/0.txt",
		"b/1.txt",
		"b",
		"a",
	}
	e := fstest.TestFS(merged, expectedFiles...)
	if e != nil {
		t.Logf("TestFS failed: %s\n", e)
		t.FailNow()
	}
	// A short white-box test to make sure we've cached some paths.
	cachedCount := len(merged.knownOKPrefixes)
	if cachedCount == 0 {
		t.Logf("Didn't get any cached path prefixes.\n")
		t.FailNow()
	}
	t.Logf("Path prefix cache after basic FS test: %d\n", cachedCount)
	nestedMergedFS := merged.B.(*MergedFS)
	cachedCount = len(nestedMergedFS.knownOKPrefixes)
	if cachedCount == 0 {
		t.Logf("Nested MergedFS didn't get any cached path prefixes.\n")
		t.FailNow()
	}
	t.Logf("Nested MergedFS prefix cache after basic FS test: %d\n",
		cachedCount)

	// Disable caching, and make sure it cleared the cache.
	merged.UsePathCaching(false)
	cachedCount = len(merged.knownOKPrefixes)
	if cachedCount != 0 {
		t.Logf("The path prefix cache wasn't cleared after disabling "+
			"caching. Still contains %d items.\n", cachedCount)
		t.FailNow()
	}
	cachedCount = len(nestedMergedFS.knownOKPrefixes)
	if cachedCount != 0 {
		t.Logf("The prefix cache of the nested FS wasn't cleared after "+
			"disabling caching. Still contains %d items.\n", cachedCount)
		t.Fail()
	}
	// Make sure the regular test still works with caching off.
	e = fstest.TestFS(merged, expectedFiles...)
	if e != nil {
		t.Logf("Second TestFS failed: %s\n", e)
		t.FailNow()
	}
	cachedCount = len(merged.knownOKPrefixes)
	if cachedCount != 0 {
		t.Logf("The path prefix cache changed even though caching was "+
			"disabled. Contains %d items.\n", cachedCount)
		t.FailNow()
	}
	// Add a file to the test_data directory, with the same name as a directory
	// in one of the zips.
	newFile, e := os.Create("test_data/b")
	if e != nil {
		t.Logf("Failed creating test_data/b file: %s\n", e)
		t.FailNow()
	}
	newFile.Close()
	defer os.Remove("test_data/b")
	// Make sure we can no longer access the contents of the "b" directory now
	// that there's a "b" regular file.
	_, e = merged.Open("b/0.txt")
	if e == nil {
		t.Logf("Didn't get expected error when accessing a directory with " +
			"the same name as a regular file.\n")
		t.FailNow()
	}
	t.Logf("Got expected error when attempting to access a directory with "+
		"the same name as a regular file: %s\n", e)

	// Finally make sure we can reenable caching, and that the FS behaves
	// properly after the update.
	merged.UsePathCaching(true)
	expectedFiles = []string{
		"test_a.zip",
		"test_b.zip",
		"test_c.zip",
		"test1.txt",
		"test2.txt",
		"test3.txt",
		"b",
		"a",
	}
	e = fstest.TestFS(merged, expectedFiles...)
	if e != nil {
		t.Logf("Third TestFS failed: %s\n", e)
		t.FailNow()
	}
	cachedCount = len(merged.knownOKPrefixes)
	if cachedCount == 0 {
		t.Logf("Didn't cache any path prefixes after reenabling caching.\n")
		t.FailNow()
	}
	t.Logf("After reenabling caching: %d prefixes are cached.\n", cachedCount)
}

func TestDataRace(t *testing.T) {
	zip1 := openZip("test_data/test_a.zip", t)
	zip2 := openZip("test_data/test_b.zip", t)
	zip3 := openZip("test_data/test_c.zip", t)
	merged := NewMergedFS(zip1, NewMergedFS(zip2, zip3))

	// We'll use the same files as in the previous test, but here we'll try to
	// open them all concurrently.
	expectedFiles := []string{
		"test1.txt",
		"test2.txt",
		"test3.txt",
		"b/0.txt",
		"b/1.txt",
		"b",
		"a",
	}

	for _, filename := range expectedFiles {
		go func(name string) {
			f, e := merged.Open(name)
			if e != nil {
				t.Logf("Failed opening %s: %s\n", name, e)
				t.Fail()
			}
			f.Close()
		}(filename)
	}
}

func newMapFile(content string) *fstest.MapFile {
	return &fstest.MapFile{
		Data:    []byte(content),
		Mode:    0666,
		ModTime: time.Now(),
		Sys:     nil,
	}
}

// Returns a directory path containing the given number of entries. Each
// directory name will contain four random characters, generated by an rng
// using the given seed. If the number of entries is greater than 0, the
// returned string will end with a trailing "/".
func generateDeepDir(rngSeed, entries int) string {
	if entries <= 0 {
		return ""
	}
	rng := rand.New(rand.NewSource(int64(rngSeed)))
	chars := []byte("abcdefghijklmnopqrstuvwxyz")
	builder := &strings.Builder{}
	for i := 0; i < entries; i++ {
		for j := 0; j < 4; j++ {
			builder.WriteByte(chars[rng.Intn(len(chars))])
		}
		builder.WriteByte('/')
	}
	return builder.String()
}

func TestDeepNesting(t *testing.T) {
	filesPerFS := 16
	fsA := fstest.MapFS(make(map[string]*fstest.MapFile))
	fsB := fstest.MapFS(make(map[string]*fstest.MapFile))
	paths := make([]string, 0, filesPerFS*2)
	for i := 0; i < filesPerFS; i++ {
		pathA := generateDeepDir(10000+i, 500) + "test.txt"
		fsA[pathA] = newMapFile("hi there")
		paths = append(paths, pathA)
		pathB := generateDeepDir(20000+i, 500) + "test.txt"
		fsB[pathB] = newMapFile("hi there 2")
		paths = append(paths, pathB)
	}
	merged := NewMergedFS(fsA, fsB)
	e := fstest.TestFS(merged, paths...)
	if e != nil {
		t.Logf("Failed tests for FS's with deep paths: %s\n", e)
		t.FailNow()
	}
}

// Returns a MergedFS for use in benchmarking path prefix caching, along with
// a list of paths that are in the FS.
func generateDeepBenchmarkingFS() (*MergedFS, []string) {
	fsA := fstest.MapFS(make(map[string]*fstest.MapFile))
	fsB := fstest.MapFS(make(map[string]*fstest.MapFile))
	pathA := generateDeepDir(100, 1000) + "testA.txt"
	pathB := generateDeepDir(101, 1000) + "testB.txt"
	fsA[pathA] = newMapFile("hi there A")
	fsB[pathB] = newMapFile("hi there B")
	merged := NewMergedFS(fsA, fsB)
	return merged, []string{pathA, pathB}
}

// Holds common code used for benchmarking the given MergedFS under different
// prefix-caching configurations.
func runFSBenchmark(b *testing.B, merged *MergedFS, paths []string) {
	for n := 0; n < b.N; n++ {
		for _, path := range paths {
			f, e := merged.Open(path)
			if e != nil {
				b.Logf("Failed opening %16s...: %s\n", path, e)
				b.FailNow()
			}
			stats, e := f.Stat()
			if e != nil {
				b.Logf("Failed getting stats for %16s...: %s\n", path, e)
				b.FailNow()
			}
			if stats.Size() < 1 {
				b.Logf("Expected %16s... to contain at least 1 byte\n", path)
				b.FailNow()
			}
			stats = nil
			f.Close()
		}
	}
}

func BenchmarkPathPrefixCachingEnabled(b *testing.B) {
	merged, paths := generateDeepBenchmarkingFS()
	merged.UsePathCaching(true)
	runFSBenchmark(b, merged, paths)
}

func BenchmarkPathPrefixCachingDisabled(b *testing.B) {
	merged, paths := generateDeepBenchmarkingFS()
	merged.UsePathCaching(false)
	runFSBenchmark(b, merged, paths)
}
