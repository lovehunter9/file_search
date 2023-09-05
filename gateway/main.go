package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"syscall"

	"wzinc/db"
	"wzinc/inotify"
	"wzinc/rpc"

	"github.com/rs/zerolog"
	cli "gopkg.in/urfave/cli.v1"
)

var (
	OriginCommandHelpTemplate = `{{.Name}}{{if .Subcommands}} command{{end}}{{if .Flags}} [command options]{{end}} {{.ArgsUsage}}
{{if .Description}}{{.Description}}
{{end}}{{if .Subcommands}}
SUBCOMMANDS:
  {{range .Subcommands}}{{.Name}}{{with .ShortName}}, {{.}}{{end}}{{ "\t" }}{{.Usage}}
  {{end}}{{end}}{{if .Flags}}
OPTIONS:
{{range $.Flags}}   {{.}}
{{end}}
{{end}}`
)
var app *cli.App

const DefaultPort = "6317"

func init() {
	app = cli.NewApp()
	app.Version = "${VERSION}"
	app.Commands = []cli.Command{
		commandStart,
	}

	cli.CommandHelpTemplate = OriginCommandHelpTemplate
}

var commandStart = cli.Command{
	Name:   "start",
	Usage:  "start loading contract gas fee",
	Flags:  []cli.Flag{},
	Action: Start,
}

func Start(ctx *cli.Context) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	url := os.Getenv("ZINC_URI")
	if url == "" {
		zinc_host := os.Getenv("ZINC_HOST")
		zinc_port := os.Getenv("ZINC_PORT")
		url = "http://" + zinc_host + ":" + zinc_port
		if zinc_host == "" || zinc_port == "" {
			url = "http://localhost:4080"
		}
	}
	watchDir := os.Getenv("WATCH_DIR")
	if watchDir == "" {
		watchDir = "/data"
	}
	port := os.Getenv("W_PORT")
	if port == "" {
		port = DefaultPort
	}
	username := os.Getenv("ZINC_FIRST_ADMIN_USER")
	if username == "" {
		username = "admin"
	}
	password := os.Getenv("ZINC_FIRST_ADMIN_PASSWORD")
	if password == "" {
		password = "User#123"
	}
	//chatModelUri := os.Getenv("CHAT_MODEL_URI")
	//fileModelUri := os.Getenv("FILE_MODEL_URI")
	mongoUri := os.Getenv("MONGO_URI")
	if mongoUri != "" {
		db.MongoURI = mongoUri
	}
	indexerUrl := os.Getenv("INDEXER_MODEL_URI")
	inotify.IndexerUrl = indexerUrl

	db.Init()

	rpc.InitRpcService(url, port, username, password, map[string]string{
		//rpc.ChatModelName: chatModelUri,
		//rpc.FileModelName: fileModelUri,
	})

	//rpc.TestInitES()

	inotify.WatchPath(watchDir)
	go inotify.VectorCli.Run()
	contx := context.Background()
	err := rpc.RpcServer.Start(contx)
	if err != nil {
		panic(err)
	}
	waitToExit()
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func waitToExit() {
	exit := make(chan bool, 0)
	sc := make(chan os.Signal, 1)
	if !signal.Ignored(syscall.SIGHUP) {
		signal.Notify(sc, syscall.SIGHUP)
	}
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range sc {
			fmt.Printf("received exit signal:%v", sig.String())
			close(exit)
			break
		}
	}()
	<-exit
}
