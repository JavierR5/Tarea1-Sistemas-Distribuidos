package main

import(
	"fmt"
    "time"
	"math/rand"
	"context"
	"log"
	"net"

	pb "github.com/Kendovvul/Ejemplo/Proto"
	"google.golang.org/grpc"
	amqp "github.com/rabbitmq/amqp091-go"
)

var ayuda = false

func failOnError(err error,msg string){
	if err != nil {
		log.Panicf("%s:%s",msg,err)
	}
}

func estado(msg string){
	fmt.Println("Analizando estado Laboratorio...",msg)
}

func MLlegada(esc string){
	fmt.Println("Llega escuadra " ,esc," , conteniendo estallido...")
}

func MRevision(esstado string){
	fmt.Println("Revisando estado Escuadra: "+esstado)
}

func MResolucion(esc string){
	fmt.Println("Estallido contenido,Escuadra ",esc," retornando...")
}

//type server struct{}

type server struct {
	pb.UnimplementedMessageServiceServer
}

func(*server) Intercambio(ctx context.Context, req *pb.Message) (*pb.Message,error){
	body := req.GetBody()

	if body == "1"{
		MLlegada(body)
	} else if body == "2"{
		MLlegada(body)
	}

	estallido := rand.Intn(100)
 
	if estallido <= 60{
		MRevision("LISTO")
		MResolucion(body)
		res:= &pb.Message{Body: "1",}
		ayuda = true
		return res,nil
	} else{
		MRevision("NO LISTO")
		res := &pb.Message{Body:"0",}
		return res, nil
	}	
}

func Connect(){
	lis,err := net.Listen("tcp",":9000")
	failOnError(err, "Fallo al conectar gRPC")
	s := grpc.NewServer()

	for {
		fmt.Print("%t",ayuda)
		pb.RegisterMessageServiceServer(s, &server{})
		if err = s.Serve(lis); err != nil {
			panic("El server no se pudo iniciar" + err.Error())
		}
		if ayuda {
			ayuda = false
			break
		}
	}
	return 
}


func main(){
	//Parametros iniciales
	//LabName := "Laboratiorio Pripyat" //nombre del laboratorio
	qName := "Emergencias" //nombre de la cola
	hostQ := "dist067" //ip del servidor de RabbitMQ 172.17.0.1
	tdr := time.Tick(5 * time.Second)

	//Conexion con el server
	conn,err := amqp.Dial("amqp://test:test@"+hostQ+":5672") //RabbitMQ
	failOnError(err,"Fallo al conectarse a RabbitMQ")
	defer conn.Close()

	//Creacion del channel
	ch,err := conn.Channel()
	failOnError(err,"Falla al abrir un canal")
	defer ch.Close()

	//Creacion y conexion al Queue

	q, err := ch.QueueDeclare(
		qName,
		false,
		false,
		false,
		false,
		nil,
	)

	failOnError(err,"Fallo al declarar queue")



    for{
		for hora := range tdr {
			_ = hora
			llamada := rand.Intn(100)
			if llamada <= 80 {

				estado("ESTALLIDO")
				fmt.Println("SOS Enviado a Central. Esperando respuestas...")
				//envio del mensaje.
				ctx,cancel := context.WithTimeout(context.Background(),5*time.Second)
				defer cancel()
						
				err = ch.PublishWithContext(ctx,
				"",
				q.Name,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					Body: []byte("1"),
				})

				failOnError(err,"Falla al enviar el mensaje")
				break
			} else {
				estado("OK")
			}
		}
	    Connect()
	    fmt.Print("sdsada")
	}
}
