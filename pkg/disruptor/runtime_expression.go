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

package disruptor

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// RuntimeExpression is a compiled-at-registration runtime graph expression.
type RuntimeExpression string

// ExpressionCompiler compiles runtime expressions into bool evaluators.
type ExpressionCompiler interface {
	Compile(expression RuntimeExpression) (BoolExpression, error)
}

// BoolExpression evaluates an expression and converts the final result to bool.
type BoolExpression interface {
	EvaluateBool(request ExpressionRequest) (bool, error)
}

// ExpressionRequest supplies variables to an expression evaluation.
type ExpressionRequest struct {
	Context   context.Context
	Variables RuntimeVariables
}

// ExpressionValueKind identifies an expression value type.
type ExpressionValueKind uint8

const (
	// ExpressionValueInvalid is the zero value and is never produced intentionally.
	ExpressionValueInvalid ExpressionValueKind = iota
	// ExpressionValueBool represents a bool value.
	ExpressionValueBool
	// ExpressionValueInt represents a signed integer value.
	ExpressionValueInt
	// ExpressionValueUint represents an unsigned integer value.
	ExpressionValueUint
	// ExpressionValueFloat represents a floating point value.
	ExpressionValueFloat
	// ExpressionValueString represents a string value.
	ExpressionValueString
	// ExpressionValueObject represents an unsupported object value.
	ExpressionValueObject
	// ExpressionValueNil represents nil.
	ExpressionValueNil
)

// ExpressionValue is the evaluator's normalized value representation.
type ExpressionValue struct {
	Kind  ExpressionValueKind
	Value any
}

// ExpressionValueConvertRequest describes a converter request.
type ExpressionValueConvertRequest struct {
	Value any
}

// ExpressionValueConverter converts raw values into expression values.
type ExpressionValueConverter interface {
	Convert(request ExpressionValueConvertRequest) (ExpressionValue, bool, error)
}

// ExpressionValueConverterFunc adapts a function to ExpressionValueConverter.
type ExpressionValueConverterFunc func(
	request ExpressionValueConvertRequest,
) (ExpressionValue, bool, error)

// Convert calls the wrapped converter function.
func (fn ExpressionValueConverterFunc) Convert(
	request ExpressionValueConvertRequest,
) (ExpressionValue, bool, error) {
	if fn == nil {
		return ExpressionValue{}, false, nil
	}

	return fn(request)
}

// RuntimeExpressionCompilerOption configures the default expression compiler.
type RuntimeExpressionCompilerOption interface {
	applyRuntimeExpressionCompiler(config *runtimeExpressionCompilerConfig) error
}

type runtimeExpressionCompilerConfig struct {
	converters []ExpressionValueConverter
}

type runtimeExpressionCompilerOptionFunc struct {
	applyFunc func(*runtimeExpressionCompilerConfig) error
}

//nolint:unused // The method satisfies RuntimeExpressionCompilerOption and is called through the interface.
func (fn runtimeExpressionCompilerOptionFunc) applyRuntimeExpressionCompiler(
	config *runtimeExpressionCompilerConfig,
) error {
	return fn.applyFunc(config)
}

// WithExpressionValueConverter adds a custom expression value converter.
func WithExpressionValueConverter(
	converter ExpressionValueConverter,
) RuntimeExpressionCompilerOption {
	return runtimeExpressionCompilerOptionFunc{
		applyFunc: func(config *runtimeExpressionCompilerConfig) error {
			if converter == nil {
				return fmt.Errorf("%w: expression value converter is nil", ErrInvalidRuntimeExpression)
			}

			config.converters = append(config.converters, converter)
			return nil
		},
	}
}

// NewRuntimeExpressionCompiler creates the default runtime expression compiler.
func NewRuntimeExpressionCompiler(
	opts ...RuntimeExpressionCompilerOption,
) ExpressionCompiler {
	config := runtimeExpressionCompilerConfig{
		converters: defaultExpressionValueConverters(),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.applyRuntimeExpressionCompiler(&config); err != nil {
			panic(err)
		}
	}

	return runtimeExpressionCompiler(config)
}

type runtimeExpressionCompiler struct {
	converters []ExpressionValueConverter
}

func (c runtimeExpressionCompiler) Compile(
	expression RuntimeExpression,
) (BoolExpression, error) {
	tokens, err := scanRuntimeExpression(string(expression))
	if err != nil {
		return nil, err
	}
	parser := runtimeExpressionParser{
		tokens: tokens,
	}
	node, err := parser.parseExpression(1)
	if err != nil {
		return nil, err
	}
	if parser.peek().typ != runtimeTokenEOF {
		return nil, fmt.Errorf(
			"%w: unexpected token %q",
			ErrInvalidRuntimeExpression,
			parser.peek().lit,
		)
	}

	return compiledRuntimeExpression{
		root:       node,
		converters: c.converters,
	}, nil
}

type compiledRuntimeExpression struct {
	root       runtimeExpressionNode
	converters []ExpressionValueConverter
}

func (e compiledRuntimeExpression) EvaluateBool(request ExpressionRequest) (bool, error) {
	value, err := e.root.evaluate(runtimeExpressionEvalContext{
		request:    request,
		converters: e.converters,
	})
	if err != nil {
		return false, err
	}

	return expressionValueToBool(value)
}

type runtimeExpressionEvalContext struct {
	request    ExpressionRequest
	converters []ExpressionValueConverter
}

func (c runtimeExpressionEvalContext) convert(value any) (ExpressionValue, error) {
	for _, converter := range c.converters {
		converted, handled, err := converter.Convert(ExpressionValueConvertRequest{
			Value: value,
		})
		if err != nil {
			return ExpressionValue{}, err
		}
		if handled {
			return converted, nil
		}
	}

	return ExpressionValue{Kind: ExpressionValueObject, Value: value}, nil
}

type runtimeExpressionNode interface {
	evaluate(context runtimeExpressionEvalContext) (ExpressionValue, error)
}

type runtimeLiteralNode struct {
	value ExpressionValue
}

func (n runtimeLiteralNode) evaluate(
	context runtimeExpressionEvalContext,
) (ExpressionValue, error) {
	return n.value, nil
}

type runtimePathNode struct {
	path string
}

func (n runtimePathNode) evaluate(
	context runtimeExpressionEvalContext,
) (ExpressionValue, error) {
	if context.request.Variables == nil {
		return ExpressionValue{}, fmt.Errorf(
			"%w: runtime path %q has no variables",
			ErrInvalidRuntimeExpression,
			n.path,
		)
	}
	value, ok := context.request.Variables.Lookup(n.path)
	if !ok {
		return ExpressionValue{}, fmt.Errorf(
			"%w: runtime path %q not found",
			ErrInvalidRuntimeExpression,
			n.path,
		)
	}

	return context.convert(value)
}

type runtimeUnaryNode struct {
	op   string
	node runtimeExpressionNode
}

func (n runtimeUnaryNode) evaluate(
	context runtimeExpressionEvalContext,
) (ExpressionValue, error) {
	value, err := n.node.evaluate(context)
	if err != nil {
		return ExpressionValue{}, err
	}

	switch n.op {
	case "!":
		boolean, err := requireExpressionBool(value)
		if err != nil {
			return ExpressionValue{}, err
		}

		return ExpressionValue{Kind: ExpressionValueBool, Value: !boolean}, nil
	default:
		return ExpressionValue{}, fmt.Errorf(
			"%w: unsupported unary operator %q",
			ErrInvalidRuntimeExpression,
			n.op,
		)
	}
}

type runtimeBinaryNode struct {
	op    string
	left  runtimeExpressionNode
	right runtimeExpressionNode
}

func (n runtimeBinaryNode) evaluate(
	context runtimeExpressionEvalContext,
) (ExpressionValue, error) {
	left, err := n.left.evaluate(context)
	if err != nil {
		return ExpressionValue{}, err
	}

	switch n.op {
	case "&&":
		leftBool, err := requireExpressionBool(left)
		if err != nil {
			return ExpressionValue{}, err
		}
		if !leftBool {
			return ExpressionValue{Kind: ExpressionValueBool, Value: false}, nil
		}
		right, err := n.right.evaluate(context)
		if err != nil {
			return ExpressionValue{}, err
		}
		rightBool, err := requireExpressionBool(right)
		if err != nil {
			return ExpressionValue{}, err
		}

		return ExpressionValue{Kind: ExpressionValueBool, Value: rightBool}, nil
	case "||":
		leftBool, err := requireExpressionBool(left)
		if err != nil {
			return ExpressionValue{}, err
		}
		if leftBool {
			return ExpressionValue{Kind: ExpressionValueBool, Value: true}, nil
		}
		right, err := n.right.evaluate(context)
		if err != nil {
			return ExpressionValue{}, err
		}
		rightBool, err := requireExpressionBool(right)
		if err != nil {
			return ExpressionValue{}, err
		}

		return ExpressionValue{Kind: ExpressionValueBool, Value: rightBool}, nil
	default:
		right, err := n.right.evaluate(context)
		if err != nil {
			return ExpressionValue{}, err
		}

		return evaluateRuntimeBinaryOperator(n.op, left, right)
	}
}

func evaluateRuntimeBinaryOperator(
	op string,
	left ExpressionValue,
	right ExpressionValue,
) (ExpressionValue, error) {
	switch op {
	case "==", "!=", ">", ">=", "<", "<=":
		return evaluateRuntimeComparison(op, left, right)
	case "&", "|", "^", "&^", "<<", ">>":
		return evaluateRuntimeBitwise(op, left, right)
	default:
		return ExpressionValue{}, fmt.Errorf(
			"%w: unsupported binary operator %q",
			ErrInvalidRuntimeExpression,
			op,
		)
	}
}

func evaluateRuntimeComparison(
	op string,
	left ExpressionValue,
	right ExpressionValue,
) (ExpressionValue, error) {
	if left.Kind == ExpressionValueNil || right.Kind == ExpressionValueNil {
		result := left.Kind == right.Kind
		if op == "!=" {
			result = !result
		} else if op != "==" {
			return ExpressionValue{}, fmt.Errorf(
				"%w: nil only supports == and !=",
				ErrInvalidRuntimeExpression,
			)
		}

		return ExpressionValue{Kind: ExpressionValueBool, Value: result}, nil
	}
	if isExpressionNumeric(left) && isExpressionNumeric(right) {
		leftFloat, rightFloat := expressionNumericFloat(left), expressionNumericFloat(right)
		result := compareRuntimeFloat(op, leftFloat, rightFloat)
		return ExpressionValue{Kind: ExpressionValueBool, Value: result}, nil
	}
	if left.Kind == ExpressionValueString && right.Kind == ExpressionValueString {
		result := compareRuntimeString(op, left.Value.(string), right.Value.(string))
		return ExpressionValue{Kind: ExpressionValueBool, Value: result}, nil
	}
	if left.Kind == ExpressionValueBool && right.Kind == ExpressionValueBool {
		result, err := compareRuntimeBool(op, left.Value.(bool), right.Value.(bool))
		if err != nil {
			return ExpressionValue{}, err
		}

		return ExpressionValue{Kind: ExpressionValueBool, Value: result}, nil
	}

	return ExpressionValue{}, fmt.Errorf(
		"%w: cannot compare %v and %v",
		ErrInvalidRuntimeExpression,
		left.Kind,
		right.Kind,
	)
}

func evaluateRuntimeBitwise(
	op string,
	left ExpressionValue,
	right ExpressionValue,
) (ExpressionValue, error) {
	leftUint, leftSigned, ok := expressionIntegerUint(left)
	if !ok {
		return ExpressionValue{}, fmt.Errorf(
			"%w: bitwise left operand is not an integer",
			ErrInvalidRuntimeExpression,
		)
	}
	rightUint, rightSigned, ok := expressionIntegerUint(right)
	if !ok {
		return ExpressionValue{}, fmt.Errorf(
			"%w: bitwise right operand is not an integer",
			ErrInvalidRuntimeExpression,
		)
	}

	var result uint64
	switch op {
	case "&":
		result = leftUint & rightUint
	case "|":
		result = leftUint | rightUint
	case "^":
		result = leftUint ^ rightUint
	case "&^":
		result = leftUint &^ rightUint
	case "<<":
		result = leftUint << rightUint
	case ">>":
		result = leftUint >> rightUint
	}
	if leftSigned && rightSigned && result <= math.MaxInt64 {
		return ExpressionValue{Kind: ExpressionValueInt, Value: int64(result)}, nil
	}

	return ExpressionValue{Kind: ExpressionValueUint, Value: result}, nil
}

func expressionValueToBool(value ExpressionValue) (bool, error) {
	switch value.Kind {
	case ExpressionValueBool:
		return value.Value.(bool), nil
	case ExpressionValueInt:
		return value.Value.(int64) != 0, nil
	case ExpressionValueUint:
		return value.Value.(uint64) != 0, nil
	case ExpressionValueString:
		boolean, err := strconv.ParseBool(value.Value.(string))
		if err != nil {
			return false, fmt.Errorf(
				"%w: cannot convert string %q to bool",
				ErrInvalidRuntimeExpression,
				value.Value.(string),
			)
		}

		return boolean, nil
	default:
		return false, fmt.Errorf(
			"%w: cannot convert %v to bool",
			ErrInvalidRuntimeExpression,
			value.Kind,
		)
	}
}

func requireExpressionBool(value ExpressionValue) (bool, error) {
	if value.Kind != ExpressionValueBool {
		return false, fmt.Errorf(
			"%w: logical operand must be bool, got %v",
			ErrInvalidRuntimeExpression,
			value.Kind,
		)
	}

	return value.Value.(bool), nil
}

func isExpressionNumeric(value ExpressionValue) bool {
	return value.Kind == ExpressionValueInt ||
		value.Kind == ExpressionValueUint ||
		value.Kind == ExpressionValueFloat
}

func expressionNumericFloat(value ExpressionValue) float64 {
	switch value.Kind {
	case ExpressionValueInt:
		return float64(value.Value.(int64))
	case ExpressionValueUint:
		return float64(value.Value.(uint64))
	case ExpressionValueFloat:
		return value.Value.(float64)
	default:
		return 0
	}
}

func expressionIntegerUint(value ExpressionValue) (uint64, bool, bool) {
	switch value.Kind {
	case ExpressionValueInt:
		integer := value.Value.(int64)
		if integer < 0 {
			return 0, true, false
		}

		return uint64(integer), true, true
	case ExpressionValueUint:
		return value.Value.(uint64), false, true
	default:
		return 0, false, false
	}
}

func compareRuntimeFloat(op string, left float64, right float64) bool {
	switch op {
	case "==":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	default:
		return false
	}
}

func compareRuntimeString(op string, left string, right string) bool {
	switch op {
	case "==":
		return left == right
	case "!=":
		return left != right
	case ">":
		return left > right
	case ">=":
		return left >= right
	case "<":
		return left < right
	case "<=":
		return left <= right
	default:
		return false
	}
}

func compareRuntimeBool(op string, left bool, right bool) (bool, error) {
	switch op {
	case "==":
		return left == right, nil
	case "!=":
		return left != right, nil
	default:
		return false, fmt.Errorf(
			"%w: bool only supports == and !=",
			ErrInvalidRuntimeExpression,
		)
	}
}

func defaultExpressionValueConverters() []ExpressionValueConverter {
	return []ExpressionValueConverter{
		ExpressionValueConverterFunc(convertRuntimeNilValue),
		ExpressionValueConverterFunc(convertRuntimeBoolValue),
		ExpressionValueConverterFunc(convertRuntimeSignedIntValue),
		ExpressionValueConverterFunc(convertRuntimeUnsignedIntValue),
		ExpressionValueConverterFunc(convertRuntimeFloatValue),
		ExpressionValueConverterFunc(convertRuntimeStringValue),
	}
}

func convertRuntimeNilValue(request ExpressionValueConvertRequest) (ExpressionValue, bool, error) {
	if request.Value == nil {
		return ExpressionValue{Kind: ExpressionValueNil, Value: nil}, true, nil
	}

	return ExpressionValue{}, false, nil
}

func convertRuntimeBoolValue(request ExpressionValueConvertRequest) (ExpressionValue, bool, error) {
	value, ok := request.Value.(bool)
	if !ok {
		return ExpressionValue{}, false, nil
	}

	return ExpressionValue{Kind: ExpressionValueBool, Value: value}, true, nil
}

func convertRuntimeSignedIntValue(
	request ExpressionValueConvertRequest,
) (ExpressionValue, bool, error) {
	switch value := request.Value.(type) {
	case int:
		return ExpressionValue{Kind: ExpressionValueInt, Value: int64(value)}, true, nil
	case int8:
		return ExpressionValue{Kind: ExpressionValueInt, Value: int64(value)}, true, nil
	case int16:
		return ExpressionValue{Kind: ExpressionValueInt, Value: int64(value)}, true, nil
	case int32:
		return ExpressionValue{Kind: ExpressionValueInt, Value: int64(value)}, true, nil
	case int64:
		return ExpressionValue{Kind: ExpressionValueInt, Value: value}, true, nil
	default:
		return ExpressionValue{}, false, nil
	}
}

func convertRuntimeUnsignedIntValue(
	request ExpressionValueConvertRequest,
) (ExpressionValue, bool, error) {
	switch value := request.Value.(type) {
	case uint:
		return ExpressionValue{Kind: ExpressionValueUint, Value: uint64(value)}, true, nil
	case uint8:
		return ExpressionValue{Kind: ExpressionValueUint, Value: uint64(value)}, true, nil
	case uint16:
		return ExpressionValue{Kind: ExpressionValueUint, Value: uint64(value)}, true, nil
	case uint32:
		return ExpressionValue{Kind: ExpressionValueUint, Value: uint64(value)}, true, nil
	case uint64:
		return ExpressionValue{Kind: ExpressionValueUint, Value: value}, true, nil
	case uintptr:
		return ExpressionValue{Kind: ExpressionValueUint, Value: uint64(value)}, true, nil
	default:
		return ExpressionValue{}, false, nil
	}
}

func convertRuntimeFloatValue(
	request ExpressionValueConvertRequest,
) (ExpressionValue, bool, error) {
	switch value := request.Value.(type) {
	case float32:
		return ExpressionValue{Kind: ExpressionValueFloat, Value: float64(value)}, true, nil
	case float64:
		return ExpressionValue{Kind: ExpressionValueFloat, Value: value}, true, nil
	default:
		return ExpressionValue{}, false, nil
	}
}

func convertRuntimeStringValue(
	request ExpressionValueConvertRequest,
) (ExpressionValue, bool, error) {
	value, ok := request.Value.(string)
	if !ok {
		return ExpressionValue{}, false, nil
	}

	return ExpressionValue{Kind: ExpressionValueString, Value: value}, true, nil
}

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

func scanRuntimeExpression(input string) ([]runtimeExpressionToken, error) {
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

func (s *runtimeExpressionScanner) scanPath() (runtimeExpressionToken, error) {
	start := s.pos + len("${")
	end := strings.IndexByte(s.input[start:], '}')
	if end < 0 {
		return runtimeExpressionToken{}, fmt.Errorf(
			"%w: unterminated path expression",
			ErrInvalidRuntimeExpression,
		)
	}
	path := s.input[start : start+end]
	if err := validateRuntimePath(path); err != nil {
		return runtimeExpressionToken{}, fmt.Errorf("%w: %w", ErrInvalidRuntimeExpression, err)
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
					ErrInvalidRuntimeExpression,
					raw,
				)
			}

			return runtimeExpressionToken{typ: runtimeTokenLiteral, lit: strconv.Quote(value)}, nil
		}
	}

	return runtimeExpressionToken{}, fmt.Errorf(
		"%w: unterminated string literal",
		ErrInvalidRuntimeExpression,
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
			ErrInvalidRuntimeExpression,
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
		ErrInvalidRuntimeExpression,
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
				ErrInvalidRuntimeExpression,
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
				ErrInvalidRuntimeExpression,
			)
		}

		return node, nil
	default:
		return nil, fmt.Errorf(
			"%w: unexpected token %q",
			ErrInvalidRuntimeExpression,
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
		return runtimeLiteralNode{value: ExpressionValue{Kind: ExpressionValueBool, Value: true}}, nil
	case "false":
		return runtimeLiteralNode{value: ExpressionValue{Kind: ExpressionValueBool, Value: false}}, nil
	case "nil":
		return runtimeLiteralNode{value: ExpressionValue{Kind: ExpressionValueNil, Value: nil}}, nil
	}
	if strings.HasPrefix(literal, `"`) {
		value, err := strconv.Unquote(literal)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid string literal", ErrInvalidRuntimeExpression)
		}

		return runtimeLiteralNode{
			value: ExpressionValue{Kind: ExpressionValueString, Value: value},
		}, nil
	}
	if strings.Contains(literal, ".") {
		value, err := strconv.ParseFloat(strings.ReplaceAll(literal, "_", ""), 64)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: invalid float literal %q",
				ErrInvalidRuntimeExpression,
				literal,
			)
		}

		return runtimeLiteralNode{
			value: ExpressionValue{Kind: ExpressionValueFloat, Value: value},
		}, nil
	}
	value, err := strconv.ParseInt(strings.ReplaceAll(literal, "_", ""), 0, 64)
	if err == nil {
		return runtimeLiteralNode{
			value: ExpressionValue{Kind: ExpressionValueInt, Value: value},
		}, nil
	}
	unsigned, unsignedErr := strconv.ParseUint(strings.ReplaceAll(literal, "_", ""), 0, 64)
	if unsignedErr != nil {
		return nil, fmt.Errorf(
			"%w: invalid integer literal %q",
			ErrInvalidRuntimeExpression,
			literal,
		)
	}

	return runtimeLiteralNode{
		value: ExpressionValue{Kind: ExpressionValueUint, Value: unsigned},
	}, nil
}
