package cass_models

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// ==========================================
// 1. DEFINISI STRUCT
// ==========================================

type NotificationPengguna struct {
	IDPengguna int64   `json:"id_pengguna"`
	Pengirim   string  `json:"pengirim"`
	Judul      string  `json:"judul"`
	Pesan      string  `json:"pesan"`
	Pop        float32 `json:"pop"`
	Archive    bool    `json:"archive"`
	CreatedAt  string  `json:"created_at"` // Format ISO 8601
	ExpiredAt  string  `json:"expired_at"`
	Data       struct {
		Metadata map[string]interface{} `json:"metadata"`
		Special  interface{}            `json:"special"`
	} `json:"data"`
}

type NotificationSeller struct {
	IDSeller  int64   `json:"id_seller"`
	Pengirim  string  `json:"pengirim"`
	Judul     string  `json:"judul"`
	Pesan     string  `json:"pesan"`
	Pop       float32 `json:"pop"`
	Archive   bool    `json:"archive"`
	CreatedAt string  `json:"created_at"`
	ExpiredAt string  `json:"expired_at"`
	Data      struct {
		Metadata map[string]interface{} `json:"metadata"`
		Special  interface{}            `json:"special"`
	} `json:"data"`
}

type NotificationKurir struct {
	IDKurir   int64   `json:"id_kurir"`
	Pengirim  string  `json:"pengirim"`
	Judul     string  `json:"judul"`
	Pesan     string  `json:"pesan"`
	Pop       float32 `json:"pop"`
	Archive   bool    `json:"archive"`
	CreatedAt string  `json:"created_at"`
	ExpiredAt string  `json:"expired_at"`
	Data      struct {
		Metadata map[string]interface{} `json:"metadata"`
		Special  interface{}            `json:"special"`
	} `json:"data"`
}

// ==========================================
// 2. IMPLEMENTASI NOTIFICATION PENGGUNA
// ==========================================

func (n NotificationPengguna) TableNameArchive() string {
	return "notification_pengguna_archive"
}

func (n NotificationPengguna) CreateArchiveTable(ctx context.Context, session *gocql.Session) error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id_pengguna bigint,
		pengirim text,
		judul text,
		pesan text,
		pop float,
		archive boolean,
		created_at timestamp,
		expired_at timestamp,
		data text,
		PRIMARY KEY ((id_pengguna), created_at)
	) WITH CLUSTERING ORDER BY (created_at DESC)`, n.TableNameArchive())

	if err := session.Query(query).ExecContext(ctx); err != nil {
		return fmt.Errorf("gagal membuat tabel %s: %w", n.TableNameArchive(), err)
	}
	fmt.Printf("Berhasil Eksekusi query membuat tabel %s\n", n.TableNameArchive())
	return nil
}

func (n NotificationPengguna) ParseIntoCUDType() map[string]interface{} {
	dataJSON, _ := json.Marshal(n.Data)
	createdAt, _ := time.Parse(time.RFC3339, n.CreatedAt)

	var expiredAt time.Time
	if n.ExpiredAt != "" {
		expiredAt, _ = time.Parse(time.RFC3339, n.ExpiredAt)
	}

	return map[string]interface{}{
		"id_pengguna": n.IDPengguna,
		"pengirim":    n.Pengirim,
		"judul":       n.Judul,
		"pesan":       n.Pesan,
		"pop":         n.Pop,
		"archive":     n.Archive,
		"created_at":  createdAt,
		"expired_at":  expiredAt,
		"data":        string(dataJSON),
	}
}

func (n NotificationPengguna) DropTableArchive(ctx context.Context, session *gocql.Session) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, n.TableNameArchive())
	if err := session.Query(query).ExecContext(ctx); err != nil {
		return fmt.Errorf("gagal drop tabel %s: %w", n.TableNameArchive(), err)
	}
	fmt.Printf("Berhasil drop tabel %s\n", n.TableNameArchive())
	return nil
}

// ==========================================
// 3. IMPLEMENTASI NOTIFICATION SELLER
// ==========================================

func (n NotificationSeller) TableNameArchive() string {
	return "notification_seller_archive"
}

func (n NotificationSeller) CreateArchiveTable(ctx context.Context, session *gocql.Session) error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id_seller bigint,
		pengirim text,
		judul text,
		pesan text,
		pop float,
		archive boolean,
		created_at timestamp,
		expired_at timestamp,
		data text,
		PRIMARY KEY ((id_seller), created_at)
	) WITH CLUSTERING ORDER BY (created_at DESC)`, n.TableNameArchive())

	if err := session.Query(query).ExecContext(ctx); err != nil {
		return fmt.Errorf("gagal membuat tabel %s: %w", n.TableNameArchive(), err)
	}
	fmt.Printf("Berhasil Eksekusi query membuat tabel %s\n", n.TableNameArchive())
	return nil
}

func (n NotificationSeller) ParseIntoCUDType() map[string]interface{} {
	dataJSON, _ := json.Marshal(n.Data)
	createdAt, _ := time.Parse(time.RFC3339, n.CreatedAt)

	var expiredAt time.Time
	if n.ExpiredAt != "" {
		expiredAt, _ = time.Parse(time.RFC3339, n.ExpiredAt)
	}

	return map[string]interface{}{
		"id_seller":  n.IDSeller,
		"pengirim":   n.Pengirim,
		"judul":      n.Judul,
		"pesan":      n.Pesan,
		"pop":        n.Pop,
		"archive":    n.Archive,
		"created_at": createdAt,
		"expired_at": expiredAt,
		"data":       string(dataJSON),
	}
}

func (n NotificationSeller) DropTableArchive(ctx context.Context, session *gocql.Session) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, n.TableNameArchive())
	if err := session.Query(query).ExecContext(ctx); err != nil {
		return fmt.Errorf("gagal drop tabel %s: %w", n.TableNameArchive(), err)
	}
	fmt.Printf("Berhasil drop tabel %s\n", n.TableNameArchive())
	return nil
}

// ==========================================
// 4. IMPLEMENTASI NOTIFICATION KURIR
// ==========================================

func (n NotificationKurir) TableNameArchive() string {
	return "notification_kurir_archive"
}

func (n NotificationKurir) CreateArchiveTable(ctx context.Context, session *gocql.Session) error {
	query := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id_kurir bigint,
		pengirim text,
		judul text,
		pesan text,
		pop float,
		archive boolean,
		created_at timestamp,
		expired_at timestamp,
		data text,
		PRIMARY KEY ((id_kurir), created_at)
	) WITH CLUSTERING ORDER BY (created_at DESC)`, n.TableNameArchive())

	if err := session.Query(query).ExecContext(ctx); err != nil {
		return fmt.Errorf("gagal membuat tabel %s: %w", n.TableNameArchive(), err)
	}
	fmt.Printf("Berhasil Eksekusi query membuat tabel %s\n", n.TableNameArchive())
	return nil
}

func (n NotificationKurir) ParseIntoCUDType() map[string]interface{} {
	dataJSON, _ := json.Marshal(n.Data)
	createdAt, _ := time.Parse(time.RFC3339, n.CreatedAt)

	var expiredAt time.Time
	if n.ExpiredAt != "" {
		expiredAt, _ = time.Parse(time.RFC3339, n.ExpiredAt)
	}

	return map[string]interface{}{
		"id_kurir":   n.IDKurir,
		"pengirim":   n.Pengirim,
		"judul":      n.Judul,
		"pesan":      n.Pesan,
		"pop":        n.Pop,
		"archive":    n.Archive,
		"created_at": createdAt,
		"expired_at": expiredAt,
		"data":       string(dataJSON),
	}
}

func (n NotificationKurir) DropTableArchive(ctx context.Context, session *gocql.Session) error {
	query := fmt.Sprintf(`DROP TABLE IF EXISTS %s`, n.TableNameArchive())
	if err := session.Query(query).ExecContext(ctx); err != nil {
		return fmt.Errorf("gagal drop tabel %s: %w", n.TableNameArchive(), err)
	}
	fmt.Printf("Berhasil drop tabel %s\n", n.TableNameArchive())
	return nil
}
