package main

import (
  "log"
  "time"
  "fmt" 
  "context"
  "google.golang.org/grpc"
  amqp "github.com/rabbitmq/amqp091-go"
  pb "github.com/Kendovvul/Ejemplo/Proto"
)

func failOnError(err error, msg string) {
  if err != nil {
    log.Panicf("%s: %s", msg, err)
  }
}

var esc1 = true
var esc2 = true


func MSolicitudRecibida(lab string){
	data := ""
	if lab == "1" {
		data = "Pipyat"
	} else if lab == "2" {
		data = "Renca" 
	} else if lab == "3" {
		data = "Pohang"
	} else if lab == "4" {
		data = "Kampala"
	}
	fmt.Println("Mensaje Asincrono de Laboratorio ",data," leido")
}

func MEnviadoEscuadron(lab string,esc string){
	fmt.Println("Se envía escuadra ",esc," a Laboratorio ",lab )
}

func MConsulta(esc string, status string){
	fmt.Println("Status Escuada ",esc," : ",status)
}

func MRetorno(esc string,lab string){
	fmt.Println("Retorno a Central Esuadra ",esc," , Conexion Laboratorio ",lab," cerrada.")
}
/*
func MensajeU(c pb.MessageServiceClient, esc string) (status string){
	req := &pb.Message {Body: esc,}

	res,err := c.Intercambio(context.Background(),req)

	failOnError(err,"Fallo la llegada del mensaje :(")
	status = res.Body
	return status
}*/

func EnviarAyuda(ip string,esc string,lab string, contador int,escuadron bool,puerto string){

	cc,err := grpc.Dial(ip+puerto,grpc.WithInsecure())
	if err != nil{
		fmt.Println("sdfsdf")
	}
	c:= pb.NewMessageServiceClient(cc)
	for {
		res, err := c.Intercambio(context.Background(), 
			&pb.Message{
				Body: esc,
			})
	
		if err != nil {
			panic("No se puede crear el mensaje " + err.Error())
		}
		
		status := res.Body
		if status == "0"{
			MConsulta(esc,"ESTALLIDO")
			contador++
		} else if status == "1" {
			contador++
			MConsulta(esc,"OK")
			break
		}
		time.Sleep(5 * time.Second) //espera de 5 segundos
	}
	cc.Close()
	MRetorno(esc,lab)
	escuadron = true
}

func KnownLab(name string) (lab string, ip string){
	lab = ""
	ip = ""
	if name == "1"{
		lab = "Pipyat"
		ip =  "dist065" //dist065
	} else if name == "2"{
		lab = "Renca"
		ip = "dist066" //dist066 
	} else if name == "3"{
		lab = "Pohang"
		ip = "dist067" //dist067 
	} else if name == "4"{
		lab = "Kampala"
		ip = "dist068" //dist068 
	}
	return 
}

func main(){
	//Parametros Iniciales
	qName := "Emergencias" //Nombre de la cola
	hostQ := "localhost"  //Host de RabbitMQ 172.17.0.1
	contador := 0
	//Conexion con el Server y Queue
	conn,err := amqp.Dial("amqp://guest:guest@"+hostQ+":5672")
	failOnError(err,"Fallo al conectar con RabbitMQ")
	defer conn.Close()

	//Conexion Labs
	
	

	ch,err := conn.Channel()
	failOnError(err,"Fallo al abrir el canal")
	defer ch.Close()

	//Escuadras
	esc1 := true
	esc2 := true

	q,err := ch.QueueDeclare(
		qName,
		false,
		false,
		false,
		false,
		nil,
	)

	failOnError(err,"Fallo al declarar Queue")
	
	//Recibir los mensajes
	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	failOnError(err,"Fallo al consumir el mensaje")

	//var forever chan struct{}

	for delivery := range msgs {
		//Enviar Escuadras
		if esc1||esc2 == true{
			data := string(delivery.Body[:])
			MSolicitudRecibida(data)
			if esc1 == true{
				contador = 0
				esc1 = false
				lab1 := data // "1"
				MEnviadoEscuadron(lab1,"1")
				name,ip := KnownLab(lab1)
				go EnviarAyuda(ip,"1",name,contador,esc1,":9000")
			} else {
				contador = 0
				esc2 = false
				lab2 := data
				MEnviadoEscuadron(lab2,"2")
				name,ip := KnownLab(lab2)
				go EnviarAyuda(ip,"2",name,contador,esc2,":9000")
			}

		}
		//<------------------------------------------------------------------------->
	}
	//<-forever
}
