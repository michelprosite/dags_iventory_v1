# Preparação para o ambiente Docker

## Certifique-se de que não existe nenhuma versão antiga instalada removendo-a

	sudo apt-get remove docker docker-engine docker.io containerd runc

## Atualize o repositório

	sudo apt-get update

## Instale pacotes necessários

	sudo apt-get install apt-transport-https ca-certificates curl gnupg-agent software-properties-common -y

## Adicione a chave oficial do Docker

	curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -

## Verifique se a chave foi adicionada

	sudo apt-key fingerprint 0EBFCD88

## Adicione o repositório do Docker

	sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

## Instale o Docker

	sudo apt-get install docker-ce docker-ce-cli containerd.io -y

## Verifique se o Docker foi corretamente instalado executando no terminal

	docker version

## Vamos adicionar o nosso usuário ao grupo do docker

	sudo usermod -aG docker $USER

	newgrp docker

	OBS: Por quê fizemos isso?

	Fizemos isso porque caso contrário só poderíamos executar o docker como superusuário, sudo ou root.

	Não queremos isso. Queremos que nosso usuário comum possa executar o Docker. 

	Por isso adicionamos nosso usuário ao grupo docker.

	Quando o docker é executado, ele procura por um grupo com seu nome no sistema e dá permissão de uso aos
	usuários que fazem parte deste grupo.

	OBS: O segundo comando é 'uma gambiarra' que adiciona o nosso usuário a um grupo 'virtual' que demos
	o nome de docker para evitar que tenhamos que reiniciar o computador.


## Testar a instalação executando o Hello World no terminal

	docker run hello-world



> Tudo pronto para a diversão!
