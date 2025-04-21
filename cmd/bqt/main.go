package main

import (
	"fmt"
	"log"
	"os"

	"github.com/JoseTorrado/bqt/internal/test"

	cli "github.com/urfave/cli/v2"
)

func main() {
	app := &cli.App{
		Name:  "bqt",
		Usage: "Run tests using a local BQ emulator",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "mode",
				Value:    "local",
				Usage:    "`local` (default) runs your test on a BQ emulator. 'cloud': runs your queries on the cloud (disabled)",
				Required: false,
			},
		},
		Action: func(cCtx *cli.Context) error {
			// Default to current directory if no argument provided
			testsPath := "."
			if cCtx.NArg() > 0 {
				testsPath = cCtx.Args().Get(0)
			}

			mode := cCtx.String("mode")

			fmt.Println("Parsing tests in directory:", testsPath)
			tests, err := test.ParseFolder(testsPath)
			if err != nil {
				return err
			}
			fmt.Println("Parsed Tests:", len(tests))
			fmt.Println("Running Tests...")
			err = test.RunTests(mode, tests)
			if err != nil {
				return err
			}
			return nil
		},
	}

	// For backward compatibility, keep the test command but make it do the same thing
	app.Commands = []*cli.Command{
		{
			Name:    "test",
			Aliases: []string{"t"},
			Usage:   "Run tests using a local BQ emulator",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "tests",
					Value:    "unit_tests/",
					Usage:    "Path to your folder containing yaml test definitions",
					Required: false,
				},
				&cli.StringFlag{
					Name:     "mode",
					Value:    "local",
					Usage:    "`local` (default) runs your test on a BQ emulator. 'cloud': runs your queries on the cloud (disabled)",
					Required: false,
				},
			},
			Action: func(cCtx *cli.Context) error {
				mode := cCtx.String("mode")
				testsPath := cCtx.String("tests")
				fmt.Println("Parsing tests in directory:", testsPath)
				tests, err := test.ParseFolder(testsPath)
				if err != nil {
					return err
				}
				fmt.Println("Parsed Tests:", len(tests))
				fmt.Println("Running Tests...")
				err = test.RunTests(mode, tests)
				if err != nil {
					return err
				}
				return nil
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
