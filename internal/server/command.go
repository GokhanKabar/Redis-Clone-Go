package server

import (
	"strings"

	"redis-clone/internal/protocol"
)

func (s *Server) executeCommand(cmd *protocol.RESPValue) *protocol.RESPValue {
	if cmd.Type != protocol.Array || len(cmd.Array) == 0 {
		return &protocol.RESPValue{
			Type: protocol.Error,
			Str:  "ERR invalid command format",
		}
	}

	command := strings.ToUpper(cmd.Array[0].Str)
	args := make([]string, len(cmd.Array)-1)
	for i, arg := range cmd.Array[1:] {
		args[i] = arg.Str
	}

	// Log command for AOF
	if isWriteCommand(command) {
		cmdStr := command
		for _, arg := range args {
			cmdStr += " " + arg
		}
		s.persistence.WriteAOF(cmdStr)
	}

	switch command {
	case "PING":
		return s.handlePing(args)
	case "SET":
		return s.handleSet(args)
	case "GET":
		return s.handleGet(args)
	case "DEL":
		return s.handleDel(args)
	default:
		return &protocol.RESPValue{
			Type: protocol.Error,
			Str:  "ERR unknown command '" + command + "'",
		}
	}
}

func isWriteCommand(command string) bool {
	writeCommands := map[string]bool{
		"SET": true,
		"DEL": true,
	}
	return writeCommands[command]
}

func (s *Server) handlePing(args []string) *protocol.RESPValue {
	if len(args) == 0 {
		return &protocol.RESPValue{
			Type: protocol.SimpleString,
			Str:  "PONG",
		}
	}
	return &protocol.RESPValue{
		Type: protocol.BulkString,
		Str:  args[0],
	}
}

func (s *Server) handleSet(args []string) *protocol.RESPValue {
	if len(args) < 2 {
		return &protocol.RESPValue{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'set' command",
		}
	}

	key, value := args[0], args[1]
	s.db.Set(key, value)

	return &protocol.RESPValue{
		Type: protocol.SimpleString,
		Str:  "OK",
	}
}

func (s *Server) handleGet(args []string) *protocol.RESPValue {
	if len(args) != 1 {
		return &protocol.RESPValue{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'get' command",
		}
	}

	key := args[0]
	value, exists := s.db.Get(key)
	if !exists {
		return &protocol.RESPValue{
			Type: protocol.BulkString,
			Null: true,
		}
	}

	return &protocol.RESPValue{
		Type: protocol.BulkString,
		Str:  value,
	}
}

func (s *Server) handleDel(args []string) *protocol.RESPValue {
	if len(args) == 0 {
		return &protocol.RESPValue{
			Type: protocol.Error,
			Str:  "ERR wrong number of arguments for 'del' command",
		}
	}

	deleted := 0
	for _, key := range args {
		if s.db.Del(key) {
			deleted++
		}
	}

	return &protocol.RESPValue{
		Type: protocol.Integer,
		Num:  int64(deleted),
	}
}
