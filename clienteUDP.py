import socket
import time
import pickle
import hashlib
import os
import threading

IP = input('Inserte la IP a la que desea conectarse (Server: 192.168.193.131): ')
PORT = int(input('Inserte el puerto al que desea conectarse: '))
clientes_paralelos = int(input('Digite el numero de clientes con el que compartira la conexion (incluyendolo a usted): '))
nombre_archivo = input('Ingrese nombre del archivo que recibira: ')

dir_servidor = (IP, PORT)
TAM_BUFFER = 1048576
tiempo = 0
dir_src = os.getcwd()
dir_ArchivosRecibidos = os.path.join(dir_src,"ArchivosRecibidos")

paquetesRecibidos = []
num_recibidos = []

terminado = False

recibidos = 0
corruptos = 0

def pedirArchivo(nombre_archivo):

	global terminado
	global recibidos
	global corruptos
	global paquetesRecibidos
	global num_recibidos

	#crea el archivo vacio donde se va a escribir lo que se recibe del servidor

	if nombre_archivo == '100_file.mp4':
		enviados = 96807
	elif nombre_archivo == '250_file.mp4':
		enviados = 256570
	elif nombre_archivo == 'test_file.jpg':
		enviados = 4544

	if nombre_archivo == '100_file.mp4':
		tam_archivo = 99130288
	elif nombre_archivo == '250_file.mp4':
		tam_archivo = 262727422
	elif nombre_archivo == 'test_file.jpg':
		tam_archivo = 4653056

	contador = 0

	servidor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	servidor.sendto(pickle.dumps('Hola'), dir_servidor)

	#envia al servidor nombre del archivo a descargar
	aceptado = False

	terminado = False

	thread_integridad = threading.Thread(
		target = revisarIntegridad
	)

	thread_integridad.start()

	print("Esperando reparticion del archivo")

	data,addr = servidor.recvfrom(TAM_BUFFER)
	paquetesRecibidos.append(data)
	contador += 1
	num_recibidos.append(contador)

	tiempo = time.time()
	tiempo_inicio_str = time.strftime("%c")

	try:

		while True:
			servidor.settimeout(5)
			data,addr = servidor.recvfrom(TAM_BUFFER)
			paquetesRecibidos.append(data)
			contador += 1
			num_recibidos.append(contador)

	except Exception as inst:

		print("Termino transferencia")

	terminado = True

	print("Esperando revision de integridad")

	tiempo = time.time() - tiempo
	tiempo_final_str = time.strftime("%c")

	thread_integridad.join()

	perdidos = enviados - recibidos

	print('Termino de transferir')
	print("Tiempo de transferencia: " + str(tiempo-5) + " segundos")
	archivocompleto = "Yes"
	if corruptos > 0 or perdidos > 0:
		archivocompleto = "No"

	mensaje = ('Transferencia terminada. Inicio: {}. Fin: {}. Archivo: {}. Tamanio archivo: {}. Completo: {}. Total enviados: {}. Recibidos: {}. Corruptos: {}. Perdidos: {}. Tiempo envio: {}. Clientes al tiempo: {}'.format(tiempo_inicio_str, tiempo_final_str, nombre_archivo, str(tam_archivo) + " bytes", archivocompleto, enviados, recibidos, corruptos, perdidos, str(tiempo-5), clientes_paralelos))
	servidor.sendto(pickle.dumps(mensaje), dir_servidor)
	servidor.close()
	if clientes_paralelos == 1:
		time.sleep(5)
	elif clientes_paralelos <= 5:
		time.sleep(30)
	else:
		time.sleep(60)

def revisarIntegridad():

	global terminado
	global recibidos
	global corruptos
	global paquetesRecibidos
	global num_recibidos

	os.chdir(dir_ArchivosRecibidos)
	f = open(nombre_archivo, 'wb')

	recibidos = 0
	corruptos = 0

	while terminado == False or len(paquetesRecibidos) > 0:

		if len(paquetesRecibidos) > 0:

			data = pickle.loads(paquetesRecibidos[0])
			del(paquetesRecibidos[0])
			contador = num_recibidos[0]
			del(num_recibidos[0])
			hash_object = hashlib.md5(data[0])
			if hash_object.hexdigest() == data[1]:

				f.write(data[0])
				print("paquete correcto: " + str(contador))
				recibidos += 1
			else:
				print("paquete corrupto: " + str(contador) )
				corruptos += 1

	f.close()

if __name__ == "__main__":
	pedirArchivo(nombre_archivo)
