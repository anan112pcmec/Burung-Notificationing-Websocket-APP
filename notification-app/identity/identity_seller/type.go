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
	fmt.Println("========== VALIDATING SELLER SESSION ==========")

	fmt.Printf("[PAYLOAD]\n")
	fmt.Printf("  IdSeller    : %d\n", i.IdSeller)
	fmt.Printf("  Username    : %s\n", i.Username)
	fmt.Printf("  EmailSeller : %s\n", i.EmailSeller)

	if i.IdSeller == 0 {
		fmt.Println("[FAILED] IdSeller bernilai 0")
		return false
	}
	fmt.Println("[SUCCESS] IdSeller valid")

	if i.Username == "" {
		fmt.Println("[FAILED] Username kosong")
		return false
	}
	fmt.Println("[SUCCESS] Username valid")

	if i.EmailSeller == "" {
		fmt.Println("[FAILED] EmailSeller kosong")
		return false
	}
	fmt.Println("[SUCCESS] EmailSeller valid")

	redisKey := fmt.Sprintf(
		"session_seller_%s_%s_%s",
		strconv.Itoa(int(i.IdSeller)),
		i.Username,
		i.EmailSeller,
	)

	fmt.Println("----------------------------------------")
	fmt.Println("[REDIS]")
	fmt.Printf("Command : HGETALL\n")
	fmt.Printf("Key     : %s\n", redisKey)

	cacheSession := rds.HGetAll(ctx, redisKey).Val()

	fmt.Printf("[RESULT] %#v\n", cacheSession)

	if len(cacheSession) == 0 {
		fmt.Println("[WARNING] Redis mengembalikan map kosong")
	}

	fmt.Println("----------------------------------------")
	fmt.Println("[FIELD CHECK]")

	for k, v := range cacheSession {
		fmt.Printf("Key: %-20s Value: %s\n", k, v)
	}

	fmt.Println("----------------------------------------")
	fmt.Println("[CHECK] id_seller")

	rawID, exist := cacheSession["id_seller"]

	if !exist {
		fmt.Println("[FAILED] Key 'id_seller' tidak ditemukan di Redis")
		return false
	}

	fmt.Printf("Value id_seller : %s\n", rawID)

	idSeller, errID := strconv.Atoi(rawID)
	if errID != nil {
		fmt.Printf("[FAILED] strconv.Atoi gagal\n")
		fmt.Printf("Input : %s\n", rawID)
		fmt.Printf("Error : %v\n", errID)
		return false
	}

	fmt.Printf("[SUCCESS] id_seller berhasil di-convert = %d\n", idSeller)

	if idSeller == 0 {
		fmt.Println("[FAILED] id_seller hasil convert bernilai 0")
		return false
	}

	fmt.Println("----------------------------------------")
	fmt.Println("[SUCCESS] Seller session VALID")
	fmt.Println("========== END VALIDATION ==========")

	return true
}
