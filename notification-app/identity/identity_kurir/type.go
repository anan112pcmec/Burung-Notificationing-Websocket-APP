package identity_kurir

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type IdentitasKurir struct {
	IdKurir       int64  `json:"id_kurir"`
	UsernameKurir string `json:"username_kurir"`
	EmailKurir    string `json:"email_kurir"`
}

func (i *IdentitasKurir) GetSessionKey() string {
	return fmt.Sprintf(
		"session_kurir_%d_%s_%s",
		i.IdKurir,
		i.UsernameKurir,
		i.EmailKurir,
	)
}

func (i *IdentitasKurir) Validating(ctx context.Context, rds *redis.Client) (status bool) {
	if i.IdKurir == 0 {
		return false
	}

	if i.UsernameKurir == "" {
		return false
	}

	if i.EmailKurir == "" {
		return false
	}

	cacheSession := rds.HGetAll(ctx, i.GetSessionKey()).Val()
	if id_kurir, err_id := strconv.Atoi(cacheSession["id_kurir"]); err_id != nil {
		return false
	} else if id_kurir == 0 {
		return false
	}

	return true
}
