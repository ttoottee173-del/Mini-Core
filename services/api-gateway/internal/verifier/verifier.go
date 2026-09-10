package verifier

import (
	"fmt"

	"github.com/golang-jwt/jwt/v5"
)

// Claims — что gateway достаёт из проверенного access-токена.
type Claims struct {
	UserID string
	Role   string
}

// Verifier проверяет подпись access-токенов ПУБЛИЧНЫМ ключом auth-service.
// Это ровно тот round-trip из твоего теста RealIssuer — только ключ чужой.
type Verifier struct {
	key    jwt.Keyfunc
	parser *jwt.Parser
}

func New(publicPEM []byte) (*Verifier, error) {
	pub, err := jwt.ParseRSAPublicKeyFromPEM(publicPEM)
	if err != nil {
		return nil, fmt.Errorf("public key: %w", err)
	}
	return &Verifier{
		key:    func(*jwt.Token) (interface{}, error) { return pub, nil },
		parser: jwt.NewParser(jwt.WithValidMethods([]string{"RS256"})),
	}, nil
}

// Verify — подпись и срок живы? Тогда claims. Иначе ошибка.
func (v *Verifier) Verify(tokenStr string) (*Claims, error) {
	mc := jwt.MapClaims{}
	if _, err := v.parser.ParseWithClaims(tokenStr, mc, v.key); err != nil {
		return nil, err
	}
	sub, _ := mc["sub"].(string)
	role, _ := mc["role"].(string)
	if sub == "" {
		return nil, fmt.Errorf("нет sub в токене")
	}
	return &Claims{UserID: sub, Role: role}, nil
}
