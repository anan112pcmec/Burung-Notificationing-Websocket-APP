package identity_seller

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func (i *IdentitySeller) GetSessionKey() string {
	return fmt.Sprintf(
		"session_seller_%d_%s_%s",
		i.IdSeller,
		i.Username,
		i.EmailSeller,
	)
}

type IdentitySeller struct {
	IdSeller    int32  `json:"id_seller"`
	Username    string `json:"username_seller"`
	EmailSeller string `json:"email_seller"`
}

func (i *IdentitySeller) Validating(ctx context.Context, rds *redis.Client) (status bool) {

	if i.IdSeller == 0 {
		return false
	}

	if i.Username == "" {
		return false
	}

	if i.EmailSeller == "" {
		return false
	}

	cacheSession := rds.HGetAll(ctx, i.GetSessionKey()).Val()
	if id_seller, err_id := strconv.Atoi(cacheSession["id_seller"]); err_id != nil {
		return false
	} else if id_seller == 0 {
		return false
	}

	return true
}
