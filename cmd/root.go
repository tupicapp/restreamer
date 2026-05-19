package cmd

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{
	Use:   "irajstreamer",
	Short: "Standalone irajstreamer CLI",
}

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	rootCmd.AddCommand(NewRawSceneCommand())
	rootCmd.AddCommand(NewSwitchCommand())
}
