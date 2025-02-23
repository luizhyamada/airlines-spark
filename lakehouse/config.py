import os

current_dir = os.path.dirname(os.path.abspath(__file__))
base_input_air_cia_path = os.path.abspath(os.path.join(current_dir, "../datasets/AIR_CIA"))
base_input_vra_path = os.path.abspath(os.path.join(current_dir, "../datasets/VRA"))

bronze_air_cia_output_path = os.path.join(current_dir, "01-bronze/air_cia")
bronze_vra_output_path = os.path.join(current_dir, "01-bronze/vra")