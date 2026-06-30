package bwa_http_req

import (
	"context"

	gocql "github.com/apache/cassandra-gocql-driver/v2"

	notification_contract "burung-notificationing-app/notification-app/notification/contract"
	notification_error "burung-notificationing-app/notification-app/notification/error"
	notification_models "burung-notificationing-app/notification-app/notification/models"
	connection_models_ws "burung-notificationing-app/notification-app/websocket/connection"

)

func PenggunaNotificationInHandles(ctx context.Context, data interface{}, dataActivePengguna *connection_models_ws.ActiveConnectionsEntity, archive_db *gocql.Session) error {
	err, dataPengguna := notification_contract.NotificationManager[notification_models.NotificationPengguna]{}.ParseNotification(data)
	if err != nil {
		return notification_error.ErrorGagalParsing
	}

	if dataPengguna.IDPengguna == 0 {
		return notification_error.ErrorDataTidakCocok
	}

	dataActivePengguna.SendNotificationDirect(data, dataPengguna.IDPengguna)

	return nil
}

func SellerNotificationInHandles(ctx context.Context, data interface{}, dataActiveSeller *connection_models_ws.ActiveConnectionsEntity, archive_db *gocql.Session) error {
	// Parse menggunakan model Notification untuk Seller (atau tetap pake model umum lu jika satu struct)
	err, dataSeller := notification_contract.NotificationManager[notification_models.NotificationSeller]{}.ParseNotification(data)
	if err != nil {
		return notification_error.ErrorGagalParsing
	}

	// Validasi ID Pengguna si Seller, pastikan gak kosong
	if dataSeller.IDSeller == 0 {
		return notification_error.ErrorDataTidakCocok
	}

	// Tembak langsung secara real-time ke koneksi WebSocket seller yang aktif
	dataActiveSeller.SendNotificationDirect(data, dataSeller.IDSeller)

	return nil
}

func KurirNotificationInHandles(ctx context.Context, data interface{}, dataActiveKurir *connection_models_ws.ActiveConnectionsEntity, archive_db *gocql.Session) error {
	// Parse menggunakan model Notification untuk Kurir
	err, dataKurir := notification_contract.NotificationManager[notification_models.NotificationKurir]{}.ParseNotification(data)
	if err != nil {
		return notification_error.ErrorGagalParsing
	}

	// Validasi ID Pengguna si Kurir, pastikan gak kosong
	if dataKurir.IDKurir == 0 {
		return notification_error.ErrorDataTidakCocok
	}

	// Tembak langsung secara real-time ke koneksi WebSocket kurir yang aktif
	dataActiveKurir.SendNotificationDirect(data, dataKurir.IDKurir)

	return nil
}
