from utils.utils import read_app_config, read_args, configure_logger


def startup():
    args = read_args()
    configure_logger(args)
    return read_app_config(args)
