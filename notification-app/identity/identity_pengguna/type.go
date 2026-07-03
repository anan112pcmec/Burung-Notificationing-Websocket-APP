package identity_pengguna

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type IdentityPengguna struct {
	ID       int64  `json:"id_pengguna"`
	Username string `json:"username_pengguna"`
	Email    string `json:"email_pengguna"`
}

func (i *IdentityPengguna) GetSessionKey() string {
	return fmt.Sprintf(
		"session_user_%d_%s_%s",
		i.ID,
		i.Username,
		i.Email,
	)
}

func (i *IdentityPengguna) Validating(ctx context.Context, rds *redis.Client) (status bool) {

	if i.ID == 0 {
		return false
	}

	if i.Username == "" {
		return false
	}

	if i.Email == "" {
		return false
	}

	cacheSession := rds.HGetAll(ctx, i.GetSessionKey()).Val()
	if id_pengguna, err_id := strconv.Atoi(cacheSession["id_user"]); err_id != nil {
		return false
	} else if id_pengguna == 0 {
		return false
	}

	return true
}
