import configparser

def create_config_file():
    config = configparser.ConfigParser()
    config['mysql'] = {}

    config['mysql']['user'] = input("Enter MySQL username: ")
    config['mysql']['password'] = input("Enter MySQL password: ")
    config['mysql']['host'] = input("Enter MySQL host (localhost if local):")
    config['mysql']['database'] = input("Enter MySQL database name:")

    with open( 'database.ini', 'w') as configfile:
        config.write(configfile)

create_config_file()

