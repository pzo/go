// TODO:
package main

import (
	"crypto/md5"
	"crypto/sha1"
	"database/sql"
	"syscall"
	"fmt"
	"github.com/jessevdk/go-flags"
	_ "github.com/mattn/go-sqlite3"
	"hash"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime/pprof"
	"time"
)

const (
	fsError = 1 << iota // error while reading entry
	fsNoHash
)

const (
	modeCreate = iota
	modeList
)

type fileData struct {
	scanID   int32
	id       int32
	parentID int32
	name     string
	status   int32
	size     int64
	mode     os.FileMode
	modtime  time.Time
	md5      []byte
	sha1     []byte
}

type perfStats struct {
	files uint32
	dirs  uint32
	bytes int64
	start time.Time
}

type createCommand struct {
	NoHash bool `short:"n" long:"nohash" description:"Don't hash files"`
}
type compareCommand struct {
	Force bool `short:"f" long:"force" description:"Force removal of files"`
}
type listCommand struct {
	Force bool `short:"u" long:"duplicates" description:"List files with same contents only"`
}

type options struct {
	// Slice of bool will append 'true' each time the option
	// is encountered (can be set multiple times, like -vvv)
	Verbose []bool `short:"v" long:"verbose" description:"Show verbose debug information"`

	CPUProfile string            `short:"p" long:"cpuprofile" description:"write cpu profile to file"`
	BaseDirs   map[string]string `short:"d" long:"directory" description:"Start directory(ies)"" optional:"yes" default:"."`

}

var (
	commonOpt  options
	createCmd  createCommand
	compareCmd compareCommand
	listCmd    listCommand
	parser     = flags.NewParser(&commonOpt, flags.Default)

	dbname string
	db     *sql.DB
	dbtx   *sql.Tx
	dberr  error
)

func dbInit(dbname string, mode int) error {
	if len(commonOpt.Verbose) > 0 {
		fmt.Println("using db :", dbname)
	}
	db, dberr = sql.Open("sqlite3", dbname)
	if dberr != nil {
		return fmt.Errorf("unable to open the database: %s", dberr)
	}
	execsql := func(sql string) {
		if _, dberr := db.Exec(sql); dberr != nil {
			fmt.Println(sql, dberr)
		}
	}
	execsql(`pragma pagesize=4096`)
	//execsql(`pragma cache_size=4000`)
	execsql(`CREATE TABLE IF NOT EXISTS scan(
id 	INTEGER PRIMARY KEY AUTOINCREMENT, 
dir text,
time text not null)`)
	execsql(`CREATE TABLE IF NOT EXISTS file(
scan_id	integer not null,
id INTEGER primary key AUTOINCREMENT,
parent_id INTEGER,
name 	text not null, 
status  integer,
size 	integer,
mode 	integer,
modtime blob,
md5 	blob,
sha1 	blob)`)
	if mode&modeList == modeList {
		execsql(`CREATE UNIQUE INDEX IF NOT EXISTS file_id on file (scan_id, id)`)
		execsql(`CREATE INDEX IF NOT EXISTS file_md5 on file (scan_id, md5)`)
		execsql(`CREATE INDEX IF NOT EXISTS file_sha1 on file (scan_id, sha1)`)
	} else if mode&modeCreate == modeCreate {
		execsql(`DROP INDEX IF EXISTS file_id`)
		execsql(`drop INDEX IF EXISTS file_md5`)
		execsql(`drop INDEX IF EXISTS file_sha1`)
	}

	return nil
}

// walk recursively descends path
func walk(path string, scanID int32, parentID int32, stats *perfStats, NoHash bool) {
	info, err := os.Lstat(path)
	if err != nil {
		checkFile(path, nil, err, scanID, parentID, stats, NoHash)
		return
	}

	var dirdata []fileData
	dirdata = append(dirdata, checkFile(path, info, nil, scanID, parentID, stats, NoHash))
	parentID = newFiles(dirdata)

	list, err := readDir(path)
	if err != nil {
		checkFile(path, info, err, scanID, parentID, stats, NoHash)
		return
	}
	var fdats []fileData

	for _, fileInfo := range list {
		if fileInfo.Mode()&os.ModeSymlink == 0 {
			if fileInfo.IsDir() {
				walk(filepath.Join(path, fileInfo.Name()), scanID, parentID, stats, NoHash)
			} else {
				fdats = append(fdats, checkFile(filepath.Join(path, fileInfo.Name()), fileInfo, nil, scanID, parentID, stats, NoHash))
			}
		}
	}
	newFiles(fdats)
	return
}

// readDir reads the directory named by dirname and returns
// a list of directory entries.
// Copied from io/ioutil, removed sort
func readDir(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	list, err := f.Readdir(-1)
	f.Close()
	if err != nil {
		return nil, err
	}
	return list, nil
}

func checkFile(fpath string, f os.FileInfo, err error, scanID int32, parentID int32, stats *perfStats, NoHash bool) fileData {
	var fdat fileData

	fdat.scanID = scanID
	fdat.parentID = parentID
	fdat.name = fpath
	fdat.status = 0

	if err != nil {
		fmt.Println(fpath, err)
		fdat.status |= fsError
	} else {
		fdat.size = f.Size()
		fdat.mode = f.Mode()
		fdat.modtime = f.ModTime()

		if f.IsDir() {
			//fdat.status |= fsIsDir
			stats.dirs++

		} else {
			if NoHash {
				fdat.status |= fsNoHash
			} else {
				if err, fdat.md5, fdat.sha1 = hashFile(fpath); err != nil {
					// Save error message in db
					// Filefound? same error message?
					fmt.Println("checkfile", err)
					fdat.status |= fsError
				}
			}

			stats.files++
			stats.bytes += f.Size()
		}
		// Strip leading path
		fdat.name = filepath.Base(fpath)
	}
	if len(commonOpt.Verbose) > 0 {
		fmt.Printf("%d %b %s md5 %x sha1 %x\n", stats.files, fdat.status, fdat.name, fdat.md5, fdat.sha1)
	}
	return fdat
}

func newScan(dir string) (int32, error) {
	var (
		res    sql.Result
		scanID int64
		reserr error
	)
	stmt, dberr := dbtx.Prepare("insert into scan(dir, time) values(?, ?)")
	if dberr != nil {
		return 0, dberr
	}
	defer stmt.Close()

	nowBin, _ := time.Now().MarshalBinary()
	if res, dberr = stmt.Exec(dir, nowBin); dberr != nil {
		return 0, dberr
	}
	if scanID, reserr = res.LastInsertId(); reserr != nil {
		return 0, reserr
	}
	return int32(scanID), nil
}

func newFiles(fdats []fileData) int32 {
	stmt, dberr := dbtx.Prepare("insert into file(scan_id, parent_id, name, status, size, mode, modtime, md5, sha1) values(?, ?, ?, ?, ?, ?, ?, ?, ?)")

	if dberr != nil {
		fmt.Println("newFile Prepare", dberr)
		return 0
	}
	defer stmt.Close()

	var fileID int32
	for _, fdat := range fdats {
		modtimeBin, _ := fdat.modtime.MarshalBinary()
		if res, dberr := stmt.Exec(fdat.scanID, fdat.parentID, fdat.name, fdat.status, fdat.size, fdat.mode, modtimeBin, fdat.md5, fdat.sha1); dberr != nil {
			fmt.Println("newFile Exec", fdat.name, dberr)
		} else {
			if id, reserr := res.LastInsertId(); reserr != nil {
				fmt.Println("newFile LastInsertId", fdat.name, reserr)
			} else {
				fileID = int32(id)
			}
		}
	}
	return fileID
}


func hashFile(fpath string) (error, []byte, []byte) {
	var (
		sumMD5  []byte
		sumSHA1 []byte
		// Blocks of var/const work just as well inside a function call
		writers []io.Writer
		hashes  []hash.Hash
		names   []string
		in      *os.File
		err     error
	)

	if in, err = os.Open(fpath); err != nil {
		return err, nil, nil
	}
	// Closures can be used to collect common operations into a nice, clean function call
	push := func(name string, h hash.Hash) {
		writers = append(writers, h) // a Hash is a writer, so this is easy
		hashes = append(hashes, h)
		names = append(names, name)
	}
	push("MD5", md5.New())
	push("SHA1", sha1.New())

	/*
		for i, _ := range names {
			hashes[i].Reset()
		}
	*/

	// The variadic expansion of a slice is really convenient.
	io.Copy(io.MultiWriter(writers...), in)

	for i, name := range names {
		switch name {
		case "MD5":
			sumMD5 = hashes[i].Sum([]byte(""))
		case "SHA1":
			sumSHA1 = hashes[i].Sum([]byte(""))
		}
	}
	return nil, sumMD5, sumSHA1
}

func (x *createCommand) Execute(args []string) error {
	var stats perfStats

	if commonOpt.CPUProfile != "" {
		f, err := os.Create(commonOpt.CPUProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	stats.start = time.Now()

	if dbiniterr := dbInit(dbname, modeCreate); dbiniterr != nil {
		fmt.Println("Could not open/create db", dbiniterr)
	} else {
		defer db.Close()

		dbtx, dberr = db.Begin()
		for baseDir := range commonOpt.BaseDirs {
			fmt.Printf("Scanning %s\n", baseDir)
			if scanID, dberr := newScan(baseDir); dberr != nil {
				fmt.Println("Could not get new scan_id", dberr)
			} else {
				walk(baseDir, scanID, 0, &stats, x.NoHash)
			}
		}
		dbtx.Commit()

		secs := time.Since(stats.start).Seconds()
		fmt.Printf("%d dir(s) %d file(s) %d byte(s) in %.02f sec(s) %d KB/sec", stats.dirs, stats.files, stats.bytes, secs, int64(float64(stats.bytes)/secs)/1024)
	}

	return nil
}

func (x *compareCommand) Execute(args []string) error {
	fmt.Printf("Compare (force=%v): %#v\n", x.Force, args)
	return nil
}

func (x *listCommand) Execute(args []string) error {
	if dbiniterr := dbInit(dbname, modeList); dbiniterr != nil {
		fmt.Println("Could not open/create db", dbiniterr)
	} else {
		defer db.Close()

	}
	return nil
}

func main() {
	//runtime.GOMAXPROCS(runtime.NumCPU())
	//fmt.Println(runtime.NumCPU())
	dbname = path.Join(".", filepath.Base(os.Args[0])+".db")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		for sig := range c {
			// sig is a ^C, handle it
			log.Printf("captured %v and exiting..", sig)
			if dbtx != nil {
				dbtx.Commit()
				db.Close()
			}
			os.Exit(1)
		}
	}()

	parser.AddCommand("create",
		"Create checksums",
		"The create command scans the specfied directory tree",
		&createCmd)
	parser.AddCommand("list",
		"List checksums",
		"The list command lists entries in the db.",
		&listCmd)
	parser.AddCommand("compare",
		"Compare checksums",
		"The rm command removes a file to the repository. Use -f to force removal of files.",
		&compareCmd)

	// Make some fake arguments to parse.
	/*
		args := []string{		//		"-v",
			"-d",
			"/home/pierre/tmp",
			"create",
		}
	*/

	if _, err := parser.Parse(); err != nil {
		//if _, err := parser.ParseArgs(args); err != nil {
		os.Exit(1)
	}
	return
}
