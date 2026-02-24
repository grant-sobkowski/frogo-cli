package cmd

import (
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

func init() {
	cobra.AddTemplateFunc("hasRequiredLocalFlags", hasRequiredLocalFlags)
	cobra.AddTemplateFunc("hasOptionalLocalFlags", hasOptionalLocalFlags)
	cobra.AddTemplateFunc("requiredLocalFlagUsages", requiredLocalFlagUsages)
	cobra.AddTemplateFunc("optionalLocalFlagUsages", optionalLocalFlagUsages)
	cobra.AddTemplateFunc("trimTrailingWhitespaces", trimTrailingWhitespaces)
	cobra.AddTemplateFunc("indentLines", indentLines)

	rootCmd.AddGroup(
		&cobra.Group{ID: "data", Title: "Interact with Data:"},
		&cobra.Group{ID: "topics", Title: "Manage Topics:"},
		&cobra.Group{ID: "other", Title: "Other Operations:"},
	)
	getCmd.GroupID = "data"
	putCmd.GroupID = "data"
	listTopicsCmd.GroupID = "topics"
	createTopicCmd.GroupID = "topics"
	deleteTopicCmd.GroupID = "topics"
	configureCmd.GroupID = "other"
	mockserverCmd.GroupID = "other"
	statusCmd.GroupID = "other"

	rootCmd.SetUsageTemplate(usageTemplate)
	rootCmd.SetHelpTemplate(`{{.UsageString}}`)
}

func isRequiredFlag(f *pflag.Flag) bool {
	_, ok := f.Annotations[cobra.BashCompOneRequiredFlag]
	return ok
}

func hasRequiredLocalFlags(cmd *cobra.Command) bool {
	found := false
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if !f.Hidden && isRequiredFlag(f) {
			found = true
		}
	})
	return found
}

func hasOptionalLocalFlags(cmd *cobra.Command) bool {
	found := false
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if !f.Hidden && !isRequiredFlag(f) {
			found = true
		}
	})
	return found
}

func filteredLocalFlagUsages(cmd *cobra.Command, required bool) string {
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	cmd.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if !f.Hidden && isRequiredFlag(f) == required {
			fs.AddFlag(f)
		}
	})
	return fs.FlagUsages()
}

func requiredLocalFlagUsages(cmd *cobra.Command) string {
	return filteredLocalFlagUsages(cmd, true)
}

func optionalLocalFlagUsages(cmd *cobra.Command) string {
	return filteredLocalFlagUsages(cmd, false)
}

func trimTrailingWhitespaces(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}
	return strings.TrimRight(strings.Join(lines, "\n"), "\n")
}

func indentLines(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, line := range lines {
		if line != "" {
			lines[i] = "  " + line
		}
	}
	return strings.Join(lines, "\n")
}

const usageTemplate = `{{.Short}}{{if .Long}}

Description:
{{.Long | trimTrailingWhitespaces | indentLines}}{{end}}

Usage:{{if .Runnable}}
  {{.UseLine}}{{end}}{{if .HasAvailableSubCommands}}
  {{.CommandPath}} [command]{{end}}{{if .HasExample}}

Examples:
{{.Example | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableSubCommands}}{{if .Groups}}{{range .Groups}}

{{.Title}}{{$g := .}}{{range $.Commands}}{{if (and .IsAvailableCommand (eq .GroupID $g.ID))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{else}}

Available Commands:{{range .Commands}}{{if (or .IsAvailableCommand (eq .Name "help"))}}
  {{rpad .Name .NamePadding }} {{.Short}}{{end}}{{end}}{{end}}{{end}}{{if hasRequiredLocalFlags .}}

Required Options:
{{requiredLocalFlagUsages . | trimTrailingWhitespaces}}{{end}}{{if hasOptionalLocalFlags .}}

Options:
{{optionalLocalFlagUsages . | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Options:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasHelpSubCommands}}

Additional help topics:{{range .Commands}}{{if .IsAdditionalHelpTopicCommand}}
  {{rpad .Name .NamePadding}} {{.Short}}{{end}}{{end}}{{end}}{{if .HasAvailableSubCommands}}

Use "{{.CommandPath}} [command] --help" for more information about a command.
{{end}}`
