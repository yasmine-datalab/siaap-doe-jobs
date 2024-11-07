# import pathlib
# import json

# cred_file = pathlib.Path(__file__).parents[2] / "config/credentials.json"
# if cred_file.exists():
#     with cred_file.open("r",) as f:
#         CONFIG = json.load(f)
# else:
#     raise RuntimeError("configs file don't exists")

# config_file = pathlib.Path(__file__).parents[2] / "config/conf.json"
# if config_file.exists():
#     with config_file.open("r",) as f:
#         CONFIG.update(json.load(f))
        
# else:
#     raise RuntimeError("configs file don't exists")