package bwa_http_req

import (
	"context"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	notification_error "burung-notificationing-app/notification-app/notification/error"
	notification_models "burung-notificationing-app/notification-app/notification/models"
	connection_models_ws "burung-notificationing-app/notification-app/websocket/connection"
)

func PenggunaNotificationInHandles(ctx context.Context, data notification_models.NotificationPengguna, dataActivePengguna *connection_models_ws.ActiveConnectionsEntity, archive_db *gocql.Session) error {

	if data.IDPengguna == 0 {
		return notification_error.ErrorDataTidakCocok
	}

	dataActivePengguna.SendNotificationDirect(data, data.IDPengguna)

	return nil
}

func SellerNotificationInHandles(ctx context.Context, data notification_models.NotificationSeller, dataActiveSeller *connection_models_ws.ActiveConnectionsEntity, archive_db *gocql.Session) error {
	// Parse menggunakan model Notification untuk Seller (atau tetap pake model umum lu jika satu struct)

	// Validasi ID Pengguna si Seller, pastikan gak kosong
	if data.IDSeller == 0 {
		return notification_error.ErrorDataTidakCocok
	}

	// Tembak langsung secara real-time ke koneksi WebSocket seller yang aktif
	dataActiveSeller.SendNotificationDirect(data, data.IDSeller)

	return nil
}

func KurirNotificationInHandles(ctx context.Context, data notification_models.NotificationKurir, dataActiveKurir *connection_models_ws.ActiveConnectionsEntity, archive_db *gocql.Session) error {
	// Parse menggunakan model Notification untuk Kurir

	// Validasi ID Pengguna si Kurir, pastikan gak kosong
	if data.IDKurir == 0 {
		return notification_error.ErrorDataTidakCocok
	}

	// Tembak langsung secara real-time ke koneksi WebSocket kurir yang aktif
	dataActiveKurir.SendNotificationDirect(data, data.IDKurir)

	return nil
}
