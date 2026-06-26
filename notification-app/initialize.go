package notification_app

import (
	"github.com/fasthttp/websocket"
	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v3"
	"github.com/gorilla/websocket"
)


func init(){

}

func RunApp() {
	fiber.New().Get("pengguna/notificationing", websocket.New(func(c *websocket.Conn) {
		defer func (){
			for key, conn := range
		}
	}))
}
