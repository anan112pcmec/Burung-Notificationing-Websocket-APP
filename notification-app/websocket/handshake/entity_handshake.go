package entity_handshake_ws

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"

	"burung-notificationing-app/notification-app/identity/identity_kurir"
	"burung-notificationing-app/notification-app/identity/identity_pengguna"
	"burung-notificationing-app/notification-app/identity/identity_seller"
	connection_models_ws "burung-notificationing-app/notification-app/websocket/connection"
	error_ws "burung-notificationing-app/notification-app/websocket/error"
)

func HandshakePengguna(path string, dataActivePengguna *connection_models_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs, session *redis.Client) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {

		fmt.Println("========== HANDSHAKE PENGGUNA ==========")
		fmt.Printf("[INFO] Connection masuk. Path: %s\n", path)

		konteks, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		var dataHandshake identity_pengguna.IdentityPengguna

		fmt.Println("[STEP 1] Menunggu payload handshake...")

		_, msg, err := c.ReadMessage()
		if err != nil {
			fmt.Printf("[FAILED] ReadMessage(): %v\n", err)
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			_ = c.Close()
			return
		}

		fmt.Printf("[SUCCESS] Payload diterima: %s\n", string(msg))

		fmt.Println("[STEP 2] Parsing JSON...")

		if err := json.Unmarshal(msg, &dataHandshake); err != nil {
			fmt.Printf("[FAILED] JSON Unmarshal: %v\n", err)
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			_ = c.Close()
			return
		}

		fmt.Printf("[SUCCESS] JSON valid -> ID=%d Username=%s\n",
			dataHandshake.ID,
			dataHandshake.Username,
		)

		fmt.Println("[STEP 3] Validasi session...")

		if valid := dataHandshake.Validating(konteks, session); !valid {
			fmt.Println("[FAILED] Session tidak valid")
			_ = c.WriteMessage(websocket.TextMessage, []byte("Anda tidak memiliki sesi yang valid"))
			_ = c.Close()
			return
		}

		fmt.Println("[SUCCESS] Session valid")

		fmt.Printf("[STEP 4] Menambahkan koneksi pengguna ID=%d\n", dataHandshake.ID)

		koneksi := &connection_models_ws.Koneksi{
			Conn: c,
		}

		dataActivePengguna.AddConnection(dataHandshake.ID, koneksi)

		fmt.Println("[SUCCESS] Koneksi berhasil ditambahkan")

		suksesMsg := fmt.Sprintf(
			"Handshake dengan pengguna %v %s Berhasil",
			dataHandshake.ID,
			dataHandshake.Username,
		)

		fmt.Println("[STEP 5] Mengirim response sukses...")

		if err := c.WriteMessage(websocket.TextMessage, []byte(suksesMsg)); err != nil {
			fmt.Printf("[FAILED] WriteMessage(): %v\n", err)
			koneksi.Disconnect()
			return
		}

		fmt.Println("[SUCCESS] Handshake pengguna selesai")
		fmt.Println("[INFO] Menunggu client disconnect...")

		for {
			_, _, err := c.ReadMessage()
			if err != nil {
				fmt.Printf("[INFO] Client disconnect: %v\n", err)
				break
			}
		}

		fmt.Println("[INFO] Membersihkan koneksi...")

		koneksi.Disconnect()

		dataActivePengguna.CleanUpClient(
			int(connection_models_ws.PenentuBagan(dataHandshake.ID))-1,
			dataHandshake.ID,
		)

		fmt.Println("[SUCCESS] Cleanup selesai")
		fmt.Println("========== END HANDSHAKE PENGGUNA ==========")
	}))
}

// =========================================================================
// HANDSHAKE SELLER
// =========================================================================
func HandshakeSeller(path string, dataActiveSeller *connection_models_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs, session *redis.Client) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {

		fmt.Println("========== HANDSHAKE SELLER ==========")
		fmt.Printf("[INFO] Connection masuk. Path: %s\n", path)

		konteks, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		var dataHandshake identity_seller.IdentitySeller

		fmt.Println("[STEP 1] Menunggu payload handshake...")

		_, msg, err := c.ReadMessage()
		if err != nil {
			fmt.Printf("[FAILED] ReadMessage(): %v\n", err)
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			_ = c.Close()
			return
		}

		fmt.Printf("[SUCCESS] Payload diterima: %s\n", string(msg))

		fmt.Println("[STEP 2] Parsing JSON...")

		if err := json.Unmarshal(msg, &dataHandshake); err != nil {
			fmt.Printf("[FAILED] JSON Unmarshal: %v\n", err)
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			_ = c.Close()
			return
		}

		fmt.Printf("[SUCCESS] JSON valid -> SellerID=%d Username=%s Email=%s\n",
			dataHandshake.IdSeller,
			dataHandshake.Username,
			dataHandshake.EmailSeller,
		)

		fmt.Println("[STEP 3] Validasi session...")

		if valid := dataHandshake.Validating(konteks, session); !valid {
			fmt.Println("[FAILED] Session tidak valid")
			_ = c.WriteMessage(websocket.TextMessage, []byte("Anda tidak memiliki sesi yang valid"))
			_ = c.Close()
			return
		}

		fmt.Println("[SUCCESS] Session valid")

		fmt.Printf("[STEP 4] Menambahkan koneksi seller ID=%d\n", dataHandshake.IdSeller)

		koneksi := &connection_models_ws.Koneksi{
			Conn: c,
		}

		dataActiveSeller.AddConnection(int64(dataHandshake.IdSeller), koneksi)

		fmt.Println("[SUCCESS] Koneksi berhasil ditambahkan")

		suksesMsg := fmt.Sprintf("Handshake dengan seller %v %s Berhasil", dataHandshake.IdSeller, dataHandshake.Username)

		fmt.Println("[STEP 5] Mengirim response sukses...")

		if err := c.WriteMessage(websocket.TextMessage, []byte(suksesMsg)); err != nil {
			fmt.Printf("[FAILED] WriteMessage(): %v\n", err)
			koneksi.Disconnect()
			return
		}

		fmt.Println("[SUCCESS] Handshake seller selesai")
		fmt.Println("[INFO] Menunggu client disconnect...")

		for {
			_, _, err := c.ReadMessage()
			if err != nil {
				fmt.Printf("[INFO] Client disconnect: %v\n", err)
				break
			}
		}

		fmt.Println("[INFO] Membersihkan koneksi...")

		koneksi.Disconnect()

		dataActiveSeller.CleanUpClient(
			int(connection_models_ws.PenentuBagan(int64(dataHandshake.IdSeller)))-1,
			int64(dataHandshake.IdSeller),
		)
		fmt.Println("[SUCCESS] Cleanup selesai")
		fmt.Println("========== END HANDSHAKE SELLER ==========")
	}))
}

// =========================================================================
// HANDSHAKE KURIR
// =========================================================================
func HandshakeKurir(path string, dataActiveKurir *connection_models_ws.ActiveConnectionsEntity, app *fiber.App, logs_error *error_ws.ErrorLogs, session *redis.Client) {
	app.Get(path, websocket.New(func(c *websocket.Conn) {

		fmt.Println("========== HANDSHAKE KURIR ==========")
		fmt.Printf("[INFO] Connection masuk. Path: %s\n", path)

		konteks, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		var dataHandshake identity_kurir.IdentitasKurir

		fmt.Println("[STEP 1] Menunggu payload handshake...")

		_, msg, err := c.ReadMessage()
		if err != nil {
			fmt.Printf("[FAILED] ReadMessage(): %v\n", err)
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			_ = c.Close()
			return
		}

		fmt.Printf("[SUCCESS] Payload diterima: %s\n", string(msg))

		fmt.Println("[STEP 2] Parsing JSON...")

		if err := json.Unmarshal(msg, &dataHandshake); err != nil {
			fmt.Printf("[FAILED] JSON Unmarshal: %v\n", err)
			logs_error.AppendError(err)
			_ = c.WriteMessage(websocket.TextMessage, []byte("Format handshake tidak valid"))
			_ = c.Close()
			return
		}

		fmt.Printf("[SUCCESS] JSON valid -> KurirID=%d Username=%s\n",
			dataHandshake.IdKurir,
			dataHandshake.UsernameKurir,
		)

		fmt.Println("[STEP 3] Validasi session...")

		if valid := dataHandshake.Validating(konteks, session); !valid {
			fmt.Println("[FAILED] Session tidak valid")
			_ = c.WriteMessage(websocket.TextMessage, []byte("Anda tidak memiliki sesi yang valid"))
			_ = c.Close()
			return
		}

		fmt.Println("[SUCCESS] Session valid")

		fmt.Printf("[STEP 4] Menambahkan koneksi kurir ID=%d\n", dataHandshake.IdKurir)

		koneksi := &connection_models_ws.Koneksi{
			Conn: c,
		}

		dataActiveKurir.AddConnection(dataHandshake.IdKurir, koneksi)

		fmt.Println("[SUCCESS] Koneksi berhasil ditambahkan")

		suksesMsg := fmt.Sprintf("Handshake dengan kurir %v %s Berhasil", dataHandshake.IdKurir, dataHandshake.UsernameKurir)

		fmt.Println("[STEP 5] Mengirim response sukses...")

		if err := c.WriteMessage(websocket.TextMessage, []byte(suksesMsg)); err != nil {
			fmt.Printf("[FAILED] WriteMessage(): %v\n", err)
			koneksi.Disconnect()
			return
		}

		fmt.Println("[SUCCESS] Handshake kurir selesai")
		fmt.Println("[INFO] Menunggu client disconnect...")

		for {
			_, _, err := c.ReadMessage()
			if err != nil {
				fmt.Printf("[INFO] Client disconnect: %v\n", err)
				break
			}
		}

		fmt.Println("[INFO] Membersihkan koneksi...")

		koneksi.Disconnect()

		dataActiveKurir.CleanUpClient(
			int(connection_models_ws.PenentuBagan(dataHandshake.IdKurir))-1,
			dataHandshake.IdKurir,
		)

		fmt.Println("[SUCCESS] Cleanup selesai")
		fmt.Println("========== END HANDSHAKE KURIR ==========")
	}))
}
