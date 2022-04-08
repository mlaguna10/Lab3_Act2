import socket
import pickle
import time
import threading
import os
import hashlib
import math

IP = input('Inserte la IP a donde desea conectar la aplicacion (local - 127.0.0.1, server - 192.168.193.131): ')
PORT = int(input('Inserte el puerto en el que desea escuchar conexiones (10000): '))
clientes_paralelos = int(input('Inserte el numero de clientes a los que atendera: '))
nombre_archivo = input('Inserte el nombre del archivo que repartira: ')

dir_servidor = (IP, PORT)
socketServidor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
socketServidor.bind(dir_servidor)
TAM_BUFFER = 1048576
TAM_MSG = 1024
dir_src = os.getcwd()
dir_archivosRecibidos = os.path.join(dir_src,"ArchivosRecibidos")
dir_archivos = os.path.join(dir_src,"Archivos")

addrs = []

def enviarObjetos(clientes):

	global clientes_paralelos

	os.chdir(dir_archivos)
	f=open(nombre_archivo, 'rb')

	#  -- algunas variables ---
	tiempo_comparacion = time.time()
	contador = 0
	condicionG = True # dice si acabo de enviar
	leerNuevo = True # dice si hay que leer mas de el archivo para reenviar (TRUE) o si es necesario reenviar (FALSE)
	dataEnv = [] #dataEnv contiene en [0] el mensaje en bytes y en [1] el mensaje hasheado como un string
	#  -------
	num_mensajes = int(math.ceil(os.path.getsize(nombre_archivo)/TAM_MSG))

	while condicionG: # si no hay nada que enviar termina
		if leerNuevo:
			# solo se cambian los datos a enviar
			dataEnv[:]=[]
			dataEnv.append(f.read(TAM_MSG)) #Guarda la porcion del archivo (datos) a mandar en el paquete
			if dataEnv[0]:
				#Guarda el hash del mensaje para verificacion de integridad
				hash_object = hashlib.md5(dataEnv[0])
				dataEnv.append(hash_object.hexdigest())
				#print("data:"+ str(dataEnv[0]) + " hash: " + dataEnv[1])
			else:
				condicionG = False
		if condicionG:
			#envia dataEnv con pickle al addr(puerto & ip)
			for i in range(clientes_paralelos):
				dir_cliente = addrs[i]
				socketServidor.sendto(pickle.dumps(dataEnv), dir_cliente)
			contador += 1
			print('Se envio paquete ' + str(contador))

	f.close()

def recibiendoClientes():

	replicas = 5
	iteraciones = 0

	while(iteraciones < replicas):

		print("Se empiezan a recibir clientes...")

		clientes_conectados = 0

		while clientes_conectados < clientes_paralelos:
			data, addr = socketServidor.recvfrom(TAM_BUFFER)
			if pickle.loads(data) == 'Hola':
				print("Nuevo cliente aceptado " + str(addr))
				addrs.append(addr)
				clientes_conectados += 1

		print("Empieza a enviarse el archivo a " + str(clientes_conectados) + " clientes")
		enviarObjetos(clientes_conectados)

		reportes_recibidos = 0

		while reportes_recibidos < clientes_paralelos:

			data, addr = socketServidor.recvfrom(TAM_BUFFER)
			thread_terminador = threading.Thread(
				target= reporte,
				args = (data, addr,)
			)
			thread_terminador.start()
			reportes_recibidos += 1

		del(addrs[:])

		iteraciones += 1

		time.sleep(2)

	socketServidor.close()

def reporte(data, addr):

	nombretxt = addr[0] + ".txt"
	os.chdir(dir_archivosRecibidos)
	file = open(nombretxt,"a")
	file.write('{}'.format(pickle.loads(data) + '\n'))
	file.close()
	print('{}'.format(pickle.loads(data)))
	os.chdir(dir_src)

if __name__ == "__main__":
	recibiendoClientes()
