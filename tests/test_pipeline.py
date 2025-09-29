from caracal.workers.worker_administrator import Pipeline
from caracal.dispatch_crew import config_parser


parser = config_parser.config_parser()
config_file = "meerkat-defaults.yml"
config, version = parser.validate_config(config_file)
opts, config = parser.update_config_from_args(config, [])

#---- pipeline config
label = "mytets"
singularity_image_dir = "sid-images"
container_tech = "singularity"

pipeline = Pipeline(config_dict=config, 
                    obsid=0,
                    container_tech=container_tech,
                    container_image_dir=singularity_image_dir)

#pipeline.add_worker("transform")
#pipeline.run_worker("transform")

pipeline.run_worker("prep")

pipeline.run_worker("flag")

pipeline.run_worker("crosscal")

pipeline.run_worker("inspect")



