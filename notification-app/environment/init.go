package environment

import "os"

func init() {
	PenggunaPathHandShake = os.Getenv("PENGGUNAPATHHANDSHAKE")
	SellerPathHandShake = os.Getenv("SELLERPATHHANDSHAKE")
	KurirPathHandShake = os.Getenv("KURIRPATHHANDSHAKE")

	PenggunaPathNotifikasiMasuk = os.Getenv("PENGGUNAPATHNOTIFIKASIMASUK")
	SellerPathHandShake = os.Getenv("SELLERPATHNOTIFIKASIMASUK")
	KurirPathHandShake = os.Getenv("KURIRPATHNOTIFIKASIMASUK")

	PortRunningAPIInNotifikasi = os.Getenv("APIINNOTIFIKASIPORT")

	PortRunningWSAppPengguna = os.Getenv("WEBSOCKETPORTPENGGUNA")
	PortRunningWSAppSeller = os.Getenv("WEBSOCKETPORTSELLER")
	PortRunningWSAppKurir = os.Getenv("WEBSOCKETPORTKURIR")

	go ErrorData.PrintError()
}
