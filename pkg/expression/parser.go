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
)

type runtimeExpressionParser struct {
	tokens []runtimeExpressionToken
	pos    int
}

func (p *runtimeExpressionParser) parseExpression(minPrecedence int) (runtimeExpressionNode, error) {
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}

	for {
		token := p.peek()
		if token.typ != runtimeTokenOperator {
			return left, nil
		}
		precedence := runtimeOperatorPrecedence(token.lit)
		if precedence < minPrecedence {
			return left, nil
		}

		p.next()
		right, err := p.parseExpression(precedence + 1)
		if err != nil {
			return nil, err
		}
		left = runtimeBinaryNode{
			op:    token.lit,
			left:  left,
			right: right,
		}
	}
}

func (p *runtimeExpressionParser) parsePrefix() (runtimeExpressionNode, error) {
	token := p.next()
	switch token.typ {
	case runtimeTokenLiteral:
		return parseRuntimeLiteral(token.lit)
	case runtimeTokenPath:
		return runtimePathNode{path: token.lit}, nil
	case runtimeTokenOperator:
		if token.lit != "!" {
			return nil, fmt.Errorf(
				"%w: unsupported prefix operator %q",
				ErrInvalid,
				token.lit,
			)
		}
		node, err := p.parseExpression(8)
		if err != nil {
			return nil, err
		}

		return runtimeUnaryNode{op: token.lit, node: node}, nil
	case runtimeTokenLeftParen:
		node, err := p.parseExpression(1)
		if err != nil {
			return nil, err
		}
		if p.next().typ != runtimeTokenRightParen {
			return nil, fmt.Errorf(
				"%w: missing closing parenthesis",
				ErrInvalid,
			)
		}

		return node, nil
	default:
		return nil, fmt.Errorf(
			"%w: unexpected token %q",
			ErrInvalid,
			token.lit,
		)
	}
}

func (p *runtimeExpressionParser) peek() runtimeExpressionToken {
	if p.pos >= len(p.tokens) {
		return runtimeExpressionToken{typ: runtimeTokenEOF}
	}

	return p.tokens[p.pos]
}

func (p *runtimeExpressionParser) next() runtimeExpressionToken {
	token := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}

	return token
}

func runtimeOperatorPrecedence(operator string) int {
	switch operator {
	case "||":
		return 1
	case "&&":
		return 2
	case "==", "!=", ">", ">=", "<", "<=":
		return 3
	case "|":
		return 4
	case "^":
		return 5
	case "&", "&^":
		return 6
	case "<<", ">>":
		return 7
	default:
		return 0
	}
}

func parseRuntimeLiteral(literal string) (runtimeExpressionNode, error) {
	switch literal {
	case "true":
		return runtimeLiteralNode{value: Value{Kind: ValueBool, Bool: true}}, nil
	case "false":
		return runtimeLiteralNode{value: Value{Kind: ValueBool, Bool: false}}, nil
	case "nil":
		return runtimeLiteralNode{value: Value{Kind: ValueNil}}, nil
	}
	if strings.HasPrefix(literal, `"`) {
		value, err := strconv.Unquote(literal)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid string literal", ErrInvalid)
		}

		return runtimeLiteralNode{
			value: Value{Kind: ValueString, String: value},
		}, nil
	}
	if strings.Contains(literal, ".") {
		value, err := strconv.ParseFloat(strings.ReplaceAll(literal, "_", ""), 64)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: invalid float literal %q",
				ErrInvalid,
				literal,
			)
		}

		return runtimeLiteralNode{
			value: Value{Kind: ValueFloat, Float: value},
		}, nil
	}
	value, err := strconv.ParseInt(strings.ReplaceAll(literal, "_", ""), 0, 64)
	if err == nil {
		return runtimeLiteralNode{
			value: Value{Kind: ValueInt, Int: value},
		}, nil
	}
	unsigned, unsignedErr := strconv.ParseUint(strings.ReplaceAll(literal, "_", ""), 0, 64)
	if unsignedErr != nil {
		return nil, fmt.Errorf(
			"%w: invalid integer literal %q",
			ErrInvalid,
			literal,
		)
	}

	return runtimeLiteralNode{
		value: Value{Kind: ValueUint, Uint: unsigned},
	}, nil
}
