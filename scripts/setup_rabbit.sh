rabbitmqctl add_user melkman melkman
rabbitmqctl add_vhost melkman
rabbitmqctl set_permissions -p melkman melkman ".*" ".*" ".*"
rabbitmqctl add_vhost melkman_test
rabbitmqctl set_permissions -p melkman_test melkman ".*" ".*" ".*"
