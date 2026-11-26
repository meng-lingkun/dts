package connector

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	maxCustomWhereBytes  = 4096
	maxCustomWhereTokens = 256
	maxCustomWhereDepth  = 16
)

type whereTokenKind uint8

const (
	whereEOF whereTokenKind = iota
	whereIdentifier
	whereLiteral
	whereAnd
	whereOr
	whereNot
	whereIs
	whereNull
	whereLike
	whereIn
	whereBetween
	whereLParen
	whereRParen
	whereComma
	whereDot
	whereCompare
)

type whereToken struct {
	kind whereTokenKind
	text string
}

// CompileCustomWhere parses a bounded predicate and renders a new SQL fragment
// from validated tokens. The qualifier, if present, must name the source table.
// Callers provide the source dialect's identifier syntax. Strings use standard
// doubled quotes and cannot contain backslashes, irrespective of session mode.
func CompileCustomWhere(expr, table string, quoteColumn func(string) string) (string, error) {
	if len(expr) > maxCustomWhereBytes || !utf8.ValidString(expr) {
		return "", fmt.Errorf("custom_where must be valid UTF-8 within %d bytes", maxCustomWhereBytes)
	}
	tokens, err := lexCustomWhere(expr)
	if err != nil {
		return "", err
	}
	p := whereParser{tokens: tokens, table: table, quoteColumn: quoteColumn}
	if err := p.parseExpression(0); err != nil {
		return "", fmt.Errorf("invalid custom_where: %w", err)
	}
	if tok := p.peek(); tok.kind != whereEOF {
		return "", fmt.Errorf("invalid custom_where: unexpected token %q", tok.text)
	}
	var sql []string
	for _, tok := range tokens {
		if tok.text != "" {
			sql = append(sql, tok.text)
		}
	}
	return "(" + strings.Join(sql, " ") + ")", nil
}

// RE2 matches a single token at the current offset. Numeric and string tokens
// are complete literals, so vendor SQL never sees the original whitespace or
// an unparsed suffix. Backslashes are excluded in every string mode.
var whereTokenPattern = regexp.MustCompile(`^(?:[A-Za-z_][A-Za-z0-9_$]*|[+-]?[0-9]+(?:\.[0-9]*)?(?:[eE][+-]?[0-9]+)?|'(?:[^'\\\x00-\x1f]|'')*'|<>|<=|>=|!=|[=<>(),.]|[ \t\r\n]+)`)

var whereSymbols = map[string]whereTokenKind{
	"AND": whereAnd, "OR": whereOr, "NOT": whereNot, "IS": whereIs,
	"NULL": whereNull, "TRUE": whereNull, "FALSE": whereNull,
	"LIKE": whereLike, "IN": whereIn, "BETWEEN": whereBetween,
	"(": whereLParen, ")": whereRParen, ",": whereComma, ".": whereDot,
	"=": whereCompare, "<>": whereCompare, "!=": whereCompare, "<": whereCompare,
	"<=": whereCompare, ">": whereCompare, ">=": whereCompare,
}

func lexCustomWhere(input string) ([]whereToken, error) {
	var tokens []whereToken
	for len(input) > 0 {
		word := whereTokenPattern.FindString(input)
		if word == "" {
			return nil, errors.New("unsupported token or incomplete literal in custom_where")
		}
		input = input[len(word):]
		switch word[0] {
		case ' ', '\n', '\r', '\t':
			continue
		}
		if len(tokens) >= maxCustomWhereTokens {
			return nil, fmt.Errorf("custom_where exceeds %d tokens", maxCustomWhereTokens)
		}
		kind := whereIdentifier
		c := word[0]
		if c == '\'' || c == '+' || c == '-' || c >= '0' && c <= '9' {
			kind = whereLiteral
		} else if symbol, ok := whereSymbols[strings.ToUpper(word)]; ok {
			kind = symbol
		}
		tokens = append(tokens, whereToken{kind: kind, text: word})
	}
	return append(tokens, whereToken{kind: whereEOF}), nil
}

type whereParser struct {
	tokens      []whereToken
	pos         int
	table       string
	quoteColumn func(string) string
}

func (p *whereParser) peek() whereToken { return p.tokens[p.pos] }
func (p *whereParser) take() whereToken { tok := p.peek(); p.pos++; return tok }
func (p *whereParser) accept(kind whereTokenKind) bool {
	if p.peek().kind != kind {
		return false
	}
	p.pos++
	return true
}

func (p *whereParser) parseExpression(depth int) error {
	if depth > maxCustomWhereDepth {
		return errors.New("predicate nesting is too deep")
	}
	if err := p.parseAnd(depth); err != nil {
		return err
	}
	for p.accept(whereOr) {
		if err := p.parseAnd(depth); err != nil {
			return err
		}
	}
	return nil
}

func (p *whereParser) parseAnd(depth int) error {
	if err := p.parsePredicate(depth); err != nil {
		return err
	}
	for p.accept(whereAnd) {
		if err := p.parsePredicate(depth); err != nil {
			return err
		}
	}
	return nil
}

func (p *whereParser) parsePredicate(depth int) error {
	p.accept(whereNot)
	if p.accept(whereLParen) {
		if err := p.parseExpression(depth + 1); err != nil {
			return err
		}
		if !p.accept(whereRParen) {
			return errors.New("missing closing parenthesis")
		}
		return nil
	}
	if err := p.parseOperand(true); err != nil {
		return err
	}
	negated := p.accept(whereNot)
	switch p.peek().kind {
	case whereCompare:
		if negated {
			return errors.New("NOT must precede IN, BETWEEN or LIKE")
		}
		p.take()
		return p.parseOperand(false)
	case whereIs:
		if negated {
			return errors.New("NOT must follow IS")
		}
		p.take()
		p.accept(whereNot)
		if !p.accept(whereNull) {
			return errors.New("IS only supports NULL, TRUE or FALSE")
		}
		return nil
	case whereLike:
		p.take()
		return p.parseLiteral()
	case whereIn:
		p.take()
		if !p.accept(whereLParen) {
			return errors.New("IN requires a parenthesized literal list")
		}
		if err := p.parseLiteral(); err != nil {
			return err
		}
		for p.accept(whereComma) {
			if err := p.parseLiteral(); err != nil {
				return err
			}
		}
		if !p.accept(whereRParen) {
			return errors.New("IN list is missing a closing parenthesis")
		}
		return nil
	case whereBetween:
		p.take()
		if err := p.parseOperand(false); err != nil {
			return err
		}
		if !p.accept(whereAnd) {
			return errors.New("BETWEEN requires AND")
		}
		return p.parseOperand(false)
	default:
		if negated {
			return errors.New("NOT must precede IN, BETWEEN or LIKE")
		}
		return errors.New("operand must be followed by a comparison predicate")
	}
}

func (p *whereParser) parseOperand(identifierAllowed bool) error {
	if !identifierAllowed && (p.peek().kind == whereLiteral || p.peek().kind == whereNull) {
		p.take()
		return nil
	}
	if !identifierAllowed {
		return errors.New("expected a literal value")
	}
	if !p.accept(whereIdentifier) {
		return errors.New("expected a source column name")
	}
	if p.accept(whereDot) {
		if !strings.EqualFold(p.tokens[p.pos-2].text, p.table) {
			return errors.New("custom_where qualifier must match the source table")
		}
		p.tokens[p.pos-2].text, p.tokens[p.pos-1].text = "", ""
		if !p.accept(whereIdentifier) {
			return errors.New("qualified identifier is incomplete")
		}
	}
	if p.quoteColumn != nil {
		col := &p.tokens[p.pos-1]
		col.text = p.quoteColumn(col.text)
	}
	return nil
}

func (p *whereParser) parseLiteral() error { return p.parseOperand(false) }

// CustomPredicate also protects batches loaded from an older metadata store.
func (req ReadBatchRequest) CustomPredicate(quote, fold func(string) string) (string, error) {
	return CompileCustomWhere(req.CustomWhere, req.Table, func(name string) string {
		if fold != nil {
			name = fold(name)
		}
		return quote(name)
	})
}
