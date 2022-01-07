import time, re, datetime
from kafka import KafkaProducer

arquivo = open(r'/var/log/apache2/access.log', 'r')
regexp = re.compile(r'[0-9]+\/+[A-za-z]+\/+[0-9]+')
produtor = KafkaProducer(bootstrap_servers='127.0.0.1:9092')

while 1:
	linha = arquivo.readline()
	if not linha:
		time.sleep(5)
	else:
		try:
			for pos in range(0, len(linha)):
				m = regexp.match(linha, pos)
				if m:
					msg = bytes(str(m), encoding='ascii')
					produtor.send('apachelog', msg)
					print('Mensagem enviada em ', datetime.datetime.now())
		except:
			print('Mensagem vazia')