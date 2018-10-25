package configure_cluster

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"

	"github.com/tamalsaha/go-oneliners"
)

type Cmd struct {
	Options CmdOptions
}

type CmdOptions struct {
	EnableStdin  bool
	EnableStdout bool
	EnableStderr bool

	Input string
}

func (o CmdOptions) SetStreamOptions(cmd *exec.Cmd, execOut, execErr *bytes.Buffer) {
	if o.EnableStdin {
		cmd.Stdin = strings.NewReader(o.Input)
	}

	if o.EnableStdout {
		cmd.Stdout = execOut
	}

	if o.EnableStderr {
		cmd.Stderr = execErr
	}
}

func NewCmdWithDefaultOptions() *Cmd {
	cmd := &Cmd{}
	cmd.Options.EnableStdout = true
	cmd.Options.EnableStderr = true

	return cmd
}

func NewCmdWithInputOptions(input string) *Cmd {
	cmd := NewCmdWithDefaultOptions()
	cmd.Options.EnableStdin = true
	cmd.Options.Input = input

	return cmd
}

func (c *Cmd) Run(command string, args ...string) (string, error) {
	var (
		cmdOut bytes.Buffer
		cmdErr bytes.Buffer
	)

	cmd := exec.Command(command, args...)
	c.Options.SetStreamOptions(cmd, &cmdOut, &cmdErr)

	fmt.Println(command, args)
	//if err := cmd.Run(); err != nil {
	err := cmd.Run()
	oneliners.PrettyJson(cmdOut.String(), "out")
	if err != nil {
		oneliners.PrettyJson(err.Error(), "err")
	}
	if cmdErr.Len() > 0 {
		oneliners.PrettyJson(cmdErr.String(), "stderr")
	}

	if err != nil {
		return "", fmt.Errorf("could not execute: %v", err)
	}

	if cmdErr.Len() > 0 {
		return "", fmt.Errorf("stderr: %v", cmdErr.String())
	}

	return cmdOut.String(), nil
}
