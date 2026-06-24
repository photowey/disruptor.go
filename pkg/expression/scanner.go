// Copyright © 2026-present The Disruptor.go Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	runtimevars "github.com/photowey/disruptor.go/pkg/runtimevars"
)

type runtimeTokenType uint8

const (
	runtimeTokenEOF runtimeTokenType = iota
	runtimeTokenLiteral
	runtimeTokenPath
	runtimeTokenOperator
	runtimeTokenLeftParen
	runtimeTokenRightParen
)

type runtimeExpressionToken struct {
	typ runtimeTokenType
	lit string
}

func scanExpression(input string) ([]runtimeExpressionToken, error) {
	scanner := runtimeExpressionScanner{input: input}
	return scanner.scan()
}

type runtimeExpressionScanner struct {
	input string
	pos   int
}

func (s *runtimeExpressionScanner) scan() ([]runtimeExpressionToken, error) {
	tokens := make([]runtimeExpressionToken, 0)
	for {
		s.skipSpace()
		if s.pos >= len(s.input) {
			tokens = append(tokens, runtimeExpressionToken{typ: runtimeTokenEOF})
			return tokens, nil
		}

		token, err := s.scanToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, token)
	}
}

func (s *runtimeExpressionScanner) scanToken() (runtimeExpressionToken, error) {
	switch ch := s.input[s.pos]; {
	case ch == '$' && s.hasPrefix("${"):
		return s.scanPath()
	case ch == '"':
		return s.scanString()
	case unicode.IsDigit(rune(ch)):
		return s.scanNumber(), nil
	case unicode.IsLetter(rune(ch)):
		return s.scanIdentifier()
	case ch == '(':
		s.pos++
		return runtimeExpressionToken{typ: runtimeTokenLeftParen, lit: "("}, nil
	case ch == ')':
		s.pos++
		return runtimeExpressionToken{typ: runtimeTokenRightParen, lit: ")"}, nil
	default:
		return s.scanOperator()
	}
}

func validateRuntimePath(path string) error {
	return runtimevars.ValidatePath(path)
}

func (s *runtimeExpressionScanner) scanPath() (runtimeExpressionToken, error) {
	start := s.pos + len("${")
	end := strings.IndexByte(s.input[start:], '}')
	if end < 0 {
		return runtimeExpressionToken{}, fmt.Errorf(
			"%w: unterminated path expression",
			ErrInvalid,
		)
	}
	path := s.input[start : start+end]
	if err := validateRuntimePath(path); err != nil {
		return runtimeExpressionToken{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	s.pos = start + end + 1

	return runtimeExpressionToken{typ: runtimeTokenPath, lit: path}, nil
}

func (s *runtimeExpressionScanner) scanString() (runtimeExpressionToken, error) {
	start := s.pos
	s.pos++
	escaped := false
	for s.pos < len(s.input) {
		ch := s.input[s.pos]
		s.pos++
		if escaped {
			escaped = false
			continue
		}
		if ch == '\\' {
			escaped = true
			continue
		}
		if ch == '"' {
			raw := s.input[start:s.pos]
			value, err := strconv.Unquote(raw)
			if err != nil {
				return runtimeExpressionToken{}, fmt.Errorf(
					"%w: invalid string literal %s",
					ErrInvalid,
					raw,
				)
			}

			return runtimeExpressionToken{typ: runtimeTokenLiteral, lit: strconv.Quote(value)}, nil
		}
	}

	return runtimeExpressionToken{}, fmt.Errorf(
		"%w: unterminated string literal",
		ErrInvalid,
	)
}

func (s *runtimeExpressionScanner) scanNumber() runtimeExpressionToken {
	start := s.pos
	for s.pos < len(s.input) {
		ch := rune(s.input[s.pos])
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '.' && ch != '_' {
			break
		}
		s.pos++
	}

	return runtimeExpressionToken{typ: runtimeTokenLiteral, lit: s.input[start:s.pos]}
}

func (s *runtimeExpressionScanner) scanIdentifier() (runtimeExpressionToken, error) {
	start := s.pos
	for s.pos < len(s.input) {
		ch := rune(s.input[s.pos])
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			break
		}
		s.pos++
	}
	literal := s.input[start:s.pos]
	switch literal {
	case "true", "false", "nil":
		return runtimeExpressionToken{typ: runtimeTokenLiteral, lit: literal}, nil
	default:
		return runtimeExpressionToken{}, fmt.Errorf(
			"%w: unexpected identifier %q",
			ErrInvalid,
			literal,
		)
	}
}

func (s *runtimeExpressionScanner) scanOperator() (runtimeExpressionToken, error) {
	for _, operator := range []string{"&&", "||", "==", "!=", ">=", "<=", "&^", "<<", ">>"} {
		if s.hasPrefix(operator) {
			s.pos += len(operator)
			return runtimeExpressionToken{typ: runtimeTokenOperator, lit: operator}, nil
		}
	}
	ch := s.input[s.pos]
	if strings.ContainsRune("!><&|^", rune(ch)) {
		s.pos++
		return runtimeExpressionToken{typ: runtimeTokenOperator, lit: string(ch)}, nil
	}

	return runtimeExpressionToken{}, fmt.Errorf(
		"%w: unexpected character %q",
		ErrInvalid,
		ch,
	)
}

func (s *runtimeExpressionScanner) skipSpace() {
	for s.pos < len(s.input) && unicode.IsSpace(rune(s.input[s.pos])) {
		s.pos++
	}
}

func (s *runtimeExpressionScanner) hasPrefix(prefix string) bool {
	return strings.HasPrefix(s.input[s.pos:], prefix)
}
