package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// See https://gowebexamples.com/advanced-middleware/
type Middleware func(http.HandlerFunc) http.HandlerFunc

// Logging logs all requests with its path and the time it took to process
func Logging() Middleware {
	// Create a new Middleware
	return func(f http.HandlerFunc) http.HandlerFunc {
		// Define the http.HandlerFunc
		return func(w http.ResponseWriter, r *http.Request) {
			// Do middleware things
			start := time.Now()
			defer func() { debugLog.Printf("%s %s took: %s", r.Method, r.URL, time.Since(start)) }()
			// Call the next middleware/handler in chain
			f(w, r)
		}
	}
}

// Method ensures that url can only be requested with a specific method, else returns a 400 Bad Request
func EnsureMethod(m string) Middleware {
	// Create a new Middleware
	return func(f http.HandlerFunc) http.HandlerFunc {
		// Define the http.HandlerFunc
		return func(w http.ResponseWriter, r *http.Request) {
			// Do middleware things
			if r.Method != m {
				http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
				return
			}
			// Call the next middleware/handler in chain
			f(w, r)
		}
	}
}

// Chain applies middlewares to a http.HandlerFunc
func Chain(f http.HandlerFunc, middlewares ...Middleware) http.HandlerFunc {
	for _, m := range middlewares {
		f = m(f)
	}
	return f
}

func HandleStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Healthy\n")
}

// Checks for a proper request.
// a) * Sees if the requested archive file is already existing in the cache.
//    * Immediately return if it exsits
// b) *
// TODO(kwk): Forward to github when s=/ or s=. ?
func HandleArchive(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	archiveMethod := vars["archiveMethod"]
	revisionOrTag := vars["revisionOrTag"]
	archiveName := vars["archiveName"]

	if archiveMethod != "tar.xz" && archiveMethod != "zip" {
		http.Error(w, fmt.Sprintf("wrong archive type (must be tar.xz or zip): %q", archiveMethod), http.StatusBadRequest)
		return
	}

	queryMap, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to parse raw query from %q: %v", r.URL.RawQuery, err), http.StatusBadRequest)
		return
	}

	set, ok := queryMap["s"]
	if !ok {
		http.Error(w, "please provide at least one ?s=path/to/a/dir/to/include/in/archive", http.StatusBadRequest)
		return
	}

	// Whether or not a fresh clone is desired.
	// Useful when a tag was updated to point at different revision.
	freshClone, _ := strconv.ParseBool(queryMap.Get("freshClone"))

	sort.Strings(set)
	setJoined := strings.Join(set, " ")

	// Calculate sha1 hash name based on revision + sparse checkout set + archive type
	// TODO(kwk): What about collisions? Too unlikely?
	h := sha1.New()
	h.Write([]byte(revisionOrTag + setJoined))
	archiveCacheName := revisionOrTag + "-" + hex.EncodeToString(h.Sum(nil)) + "." + archiveMethod

	// Serves a file as a response
	serveFile := func(filePath string) {
		w.Header().Set(http.CanonicalHeaderKey("Content-Disposition"), fmt.Sprintf("attachment; filename=%s", archiveName))
		switch archiveMethod {
		case "tar.xz":
			debugLog.Println("setting tar.xz content type")
			w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/x-tar")
			break
		case "zip":
			debugLog.Println("setting zip content type")
			w.Header().Set(http.CanonicalHeaderKey("Content-Type"), "application/zip")
		}

		http.ServeFile(w, r, filePath)
	}

	// TODO(kwk): How to prevent another handler from creating this cache file? File-based locking or just a program mutex?
	cacheFilePath := filepath.Join(localArchiveCacheDir, archiveCacheName)
	if _, err := os.Stat(cacheFilePath); !os.IsNotExist(err) {
		infoLog.Println("found archive in cache and returning it immediately.")
		serveFile(cacheFilePath)
		return
	}

	// The clone dir doesn't care about the archive extension, so create its own name
	h = sha1.New()
	h.Write([]byte(revisionOrTag + setJoined))
	cloneDirName := revisionOrTag + "-" + hex.EncodeToString(h.Sum(nil))
	clonePath := filepath.Join(gitLocalClonesDir, cloneDirName)

	if freshClone {
		infoLog.Printf("fresh clone requested, removing any old clone of revision %q in %q", revisionOrTag, clonePath)
		_ = os.RemoveAll(clonePath)
	}

	gitMirrorRWMutex.RLock()
	defer gitMirrorRWMutex.RUnlock()

	if _, err := os.Stat(clonePath); os.IsNotExist(err) || freshClone {
		log.Printf("cloning revision %q into %q (freshClone=%t)", revisionOrTag, clonePath, freshClone)
		command := `git clone --branch=main --depth=1 --filter=blob:none --sparse --reference=%[1]q %[2]s %[3]q`
		_, err := executeCommand(command, gitLocalMirrorDir, gitRepositoryURL, clonePath)
		if err != nil {
			// TODO(kwk): always log error before returning it with http.Error and also add status code to logs
			http.Error(w, fmt.Sprintf("failed to create partial clone: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		infoLog.Printf("using already existing clone of revision %q in %q (freshClone=%t)", revisionOrTag, clonePath, freshClone)
	}

	infoLog.Printf("sparse checkout of %q at revision %q in %q", setJoined, revisionOrTag, clonePath)
	command := `git -C %[1]q sparse-checkout init --cone && git -C %[1]q sparse-checkout set %s && git -C %[1]q reset --hard %[3]q`
	_, err = executeCommand(command, clonePath, setJoined, revisionOrTag)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to sparse checkout %q in %q at revision %q: %v", setJoined, clonePath, revisionOrTag, err), http.StatusInternalServerError)
		return
	}

	// TODO(kwk): Include time output for each task?
	infoLog.Printf("compressing of %q in %q into %q", setJoined, clonePath, cacheFilePath)

	switch archiveMethod {
	case "tar.xz":
		// TODO(kwk): Notice that I'm not renaming the directories like its done for LLVM releases (e.g. clang -> clang-13.0.0-src)
		command = fmt.Sprintf(`tar -C %q -cJf %q %s`, clonePath, cacheFilePath, setJoined)
	default:
		http.Error(w, fmt.Sprintf("archive method not implemented: %q", archiveMethod), http.StatusNotImplemented)
		return
	}
	_, err = executeCommand(command)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to pack %q: %v", setJoined, err), http.StatusInternalServerError)
		return
	}

	serveFile(cacheFilePath)
}

// Executes the command after formatting it with the given args.
func executeCommand(command string, args ...interface{}) ([]byte, error) {
	debugLog.Println(command)
	cmd := exec.Command("sh", "-c", command)
	stdoutStderr, err := cmd.CombinedOutput()
	debugLog.Println(string(stdoutStderr))
	if err != nil {
		return stdoutStderr, fmt.Errorf("failed to execute command %q: %w", command, err)
	}
	return stdoutStderr, nil
}

// Prints the command to be executed to the log and executes it.
// If the execution fails, a fatal log is written.
func executeCommandOrFatalLog(command string, args ...interface{}) {
	command = fmt.Sprintf(command, args...)
	debugLog.Println(command)
	cmd := exec.Command("sh", "-c", command)

	stdoutStderr, err := cmd.CombinedOutput()
	debugLog.Println(string(stdoutStderr))
	if err != nil {
		errLog.Fatal(err)
	}
}

// TODO(kwk): Cleanup: Create a handler and stuff variables in there out of global space
var (
	gitMirrorRWMutex               sync.RWMutex
	gitLocalClonesDir              string
	localArchiveCacheDir           string
	gitLocalMirrorDir              string
	gitRepositoryURL               string
	gitUpdateMirrorTickerDuration  time.Duration
	shutDownWaitDuration           time.Duration
	listenAddress                  string
	logLevel                       string
	cleanCacheTickerDuration       time.Duration
	cleanCacheRemoveFilesOlderThan time.Duration

	errLog   *log.Logger
	warnLog  *log.Logger
	infoLog  *log.Logger
	debugLog *log.Logger
)

func cleanCache() {
	fileInfo, err := ioutil.ReadDir(localArchiveCacheDir)
	if err != nil {
		errLog.Fatalf("failed to read cache dir %q: %v", localArchiveCacheDir, err)
		return
	}
	now := time.Now()
	for _, info := range fileInfo {
		if diff := now.Sub(info.ModTime()); diff > cleanCacheRemoveFilesOlderThan {
			debugLog.Printf("deleting %s which is %s old\n", info.Name(), diff)
			p := path.Join(localArchiveCacheDir, info.Name())
			err := os.Remove(p)
			if err != nil {
				errLog.Fatalf("failed to delete %q: %v", p, err)
				return
			}
		}
	}
}

func setupArchivesCache() {
	if _, err := os.Stat(localArchiveCacheDir); os.IsNotExist(err) {
		infoLog.Printf("creating archive cache directory in %q", localArchiveCacheDir)
		err := os.MkdirAll(localArchiveCacheDir, 0777)
		if err != nil {
			errLog.Fatalf("failed to create archive dir %q: %v", localArchiveCacheDir, err)
		}
	} else {
		infoLog.Printf("using already existing archive cache directory %q", localArchiveCacheDir)
	}

	// Setup ticker for updating the git mirror
	if cleanCacheTickerDuration != 0 {
		cleanCacheTicker := time.NewTicker(cleanCacheTickerDuration)
		go func() {
			for {
				select {
				case t := <-cleanCacheTicker.C:
					infoLog.Printf("cleaning cache at %s", t)
					cleanCache()
				}
			}
		}()
		defer cleanCacheTicker.Stop()
	}

}

func setupFlags() {
	flag.StringVar(&gitRepositoryURL, "git-repository-url ", "git@github.com:llvm/llvm-project.git", "What project to checkout")
	flag.StringVar(&gitLocalMirrorDir, "git-local-mirror-dir", "llvm-project.git", "Where to store the mirror of the remote repository")
	flag.DurationVar(&shutDownWaitDuration, "graceful-timeout", time.Second*15, "The duration for which the server gracefully waits for existing connections to finish - e.g. 15s or 1m")
	flag.StringVar(&listenAddress, "listen-address", "0.0.0.0:8080", "Address to start HTTP server on")
	flag.StringVar(&localArchiveCacheDir, "archive-cache-dir", "cache", "Where to store archives for later re-use")
	// Average time for a new commit in LLVM is 14 minutes
	// so we should be pretty good with a default of 10 minutes.
	flag.DurationVar(&gitUpdateMirrorTickerDuration, "git-update-mirror-timer-duration", time.Minute*10, "How often to update the local git mirror - e.g. 15s or 1m or 0 to disable")
	flag.StringVar(&gitLocalClonesDir, "git-local-clones", "clones", "Where to do the git clones")
	flag.StringVar(&logLevel, "log-level", "info", "Minimum log level (debug < info < warning < error)")
	flag.DurationVar(&cleanCacheTickerDuration, "clean-cache-ticker-duration", 1*time.Hour, "How often to run cache cleaning, e.g. 15s or 1m or 0 to disable")
	flag.DurationVar(&cleanCacheRemoveFilesOlderThan, "clean-cache-remove-files-older-than", 24*time.Hour, "Delete cache files older this duration, e.g. 15s or 1m or 0 to disable")

	flag.Parse()
}

func setupLogging() {
	// Setup and handle log-levels
	errLog = log.New(os.Stdout, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
	warnLog = log.New(os.Stdout, "WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)
	infoLog = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	debugLog = log.New(os.Stdout, "DEBUG: ", log.Ldate|log.Ltime|log.Lshortfile)
	logLevelMap := map[string]int{"error": 0, "warning": 1, "info": 2, "debug": 3}
	l, ok := logLevelMap[logLevel]
	if !ok {
		warnLog.Printf("unknown log level: %q", logLevel)
		flag.Usage()
		os.Exit(1)
	}
	if l < logLevelMap["warn"] {
		warnLog.SetOutput(ioutil.Discard)
	}
	if l < logLevelMap["info"] {
		infoLog.SetOutput(ioutil.Discard)
	}
	if l < logLevelMap["debug"] {
		debugLog.SetOutput(ioutil.Discard)
	}
}

func printConfig() {
	// Print out configration
	fmt.Printf("%-40s = %s\n", "Setting", "Value")
	fmt.Printf(strings.Repeat("-", 40) + " " + strings.Repeat("-", 40) + "\n")
	flag.VisitAll(func(f *flag.Flag) {
		fmt.Printf("%-40s = %s\n", f.Name, f.Value)
	})

	if _, err := exec.LookPath("git"); err != nil {
		errLog.Fatal("git binary not found in path")
	}
	if _, err := exec.LookPath("tar"); err != nil {
		errLog.Fatal("tar binary not found in path")
	}
}

func setupGitLocalMirror() {
	// setup local mirror
	gitLocalMirrorDir, err := filepath.Abs(gitLocalMirrorDir)
	if err != nil {
		errLog.Fatalf("failed to get absolute path for %q: %v", gitLocalMirrorDir, err)
	}

	gitMirrorRWMutex.Lock()
	go func() {
		defer gitMirrorRWMutex.Unlock()
		if _, err := os.Stat(gitLocalMirrorDir); os.IsNotExist(err) {
			infoLog.Printf("cloning local git mirror of remote %q in %q", gitRepositoryURL, gitLocalMirrorDir)
			executeCommandOrFatalLog("git clone --mirror %s %q", gitRepositoryURL, gitLocalMirrorDir)
		} else {
			infoLog.Printf("updating local git mirror of remote %q in %q", gitRepositoryURL, gitLocalMirrorDir)
			executeCommandOrFatalLog("git -C %q remote update", gitLocalMirrorDir)
		}
	}()

	// Setup ticker for updating the git mirror
	if gitUpdateMirrorTickerDuration != 0 {
		updateGitMirrorTicker := time.NewTicker(gitUpdateMirrorTickerDuration)
		go func() {
			for {
				select {
				case t := <-updateGitMirrorTicker.C:
					infoLog.Printf("updating git mirror at %s", t)
					func() {
						gitMirrorRWMutex.Lock()
						defer gitMirrorRWMutex.Unlock()
						executeCommandOrFatalLog("git -C %q remote update", gitLocalMirrorDir)
					}()
				}
			}
		}()
		defer updateGitMirrorTicker.Stop()
	}
}

func setupGitLocalClones() {
	gitLocalClonesDir, err := filepath.Abs(gitLocalClonesDir)
	if err != nil {
		errLog.Fatalf("failed to get absolute path for %q: %v", gitLocalClonesDir, err)
	}
	if _, err := os.Stat(gitLocalClonesDir); os.IsNotExist(err) {
		infoLog.Printf("creating clones directory in %q", gitLocalClonesDir)
		err := os.MkdirAll(gitLocalClonesDir, 0777)
		if err != nil {
			errLog.Fatalf("failed to create clones dir %q: %v", gitLocalClonesDir, err)
		}
	} else {
		infoLog.Printf("using already existing clones directory %q", gitLocalClonesDir)
	}
}

func main() {
	// TODO(kwk): Clean clone directory? Currently we have the ?freshClone=true URL-option to force this
	setupFlags()
	setupLogging()
	printConfig()
	setupGitLocalMirror()
	setupGitLocalClones()
	setupArchivesCache()

	infoLog.Printf("bring up HTTP server on %q\n", listenAddress)

	r := mux.NewRouter()

	// https://gowebexamples.com/advanced-middleware/
	r.HandleFunc("/archive/{archiveMethod}/{revisionOrTag}/{archiveName}/",
		Chain(HandleArchive, EnsureMethod(http.MethodGet), Logging()))

	r.HandleFunc("/status",
		Chain(HandleStatus, EnsureMethod(http.MethodGet), Logging()))

	srv := &http.Server{
		Addr: listenAddress,
		// Good practice to set timeouts to avoid Slowloris attacks.
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r, // Pass our instance of gorilla/mux in.
	}

	// Run our server in a goroutine so that it doesn't block.
	// see https://github.com/gorilla/mux#graceful-shutdown
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			errLog.Println(err)
		}
	}()

	c := make(chan os.Signal, 1)
	// We'll accept graceful shutdowns when quit via SIGINT (Ctrl+C)
	// SIGKILL, SIGQUIT or SIGTERM (Ctrl+/) will not be caught.
	signal.Notify(c, os.Interrupt)

	// Block until we receive our signal.
	<-c

	// Create a deadline to wait for.
	ctx, cancel := context.WithTimeout(context.Background(), shutDownWaitDuration)
	defer cancel()
	// Doesn't block if no connections, but will otherwise wait
	// until the timeout deadline.
	srv.Shutdown(ctx)
	// Optionally, you could run srv.Shutdown in a goroutine and block on
	// <-ctx.Done() if your application should wait for other services
	// to finalize based on context cancellation.
	infoLog.Println("Shutting down server and exiting")
	os.Exit(0)
}
