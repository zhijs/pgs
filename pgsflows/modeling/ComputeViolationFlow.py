###### ComputeViolationFlow.py ######
#!/usr/bin/env python
"""
Pipeline description documentation.

ComputeViolationFlow Arguments:
	Required Arguments:
		-i, --input_config    
		
	Optional Arguments:
		--flag	  <UNDECIDED>
"""
import argparse
import sys
import os
import json
import subprocess

from alab.pyflow import WorkflowRunner
from alab.args import add_pyflow_args
from alab.args import default_pyflow_args
from alab.args import extend_pyflow_docstring

__version__ = "0.0.1"

class ComputeViolationFlow(WorkflowRunner):
	def __init__(self, input_config):
		self.input_config = input_config

	def workflow(self):
		if type(self.input_config) == str :
			if not (os.path.exists(self.input_config) and os.access(self.input_config, os.R_OK) ):
				raise Exception('Cannot Find or Access input_config file %s' % self.input_config)
			with open(self.input_config, 'r') as data_file: 
				self.input_config = json.load(data_file)
		
		if 'source_dir' not in self.input_config :
			raise Exception('%s : Input config error, it does not have source_dir ' % os.path.name(__file__))		
			
		if 'input' not in self.input_config :
			raise Exception('%s : Input config error, it does not have input' % os.path.name(__file__))
		else :
			if 'genome' not in self.input_config['input'] :
				raise Exception('%s : Input config error, it does not have genome' % os.path.name(__file__))
			if 'resolution' not in self.input_config['input'] :
				raise Exception('%s : Input config error, it does not have resolution' % os.path.name(__file__))
					
		if 'modeling_parameters' not in self.input_config :
			raise Exception('%s : Input config error, it does not have modeling_parameters' % os.path.name(__file__))
		else :
			if 'probMat' not in self.input_config['modeling_parameters'] :
				raise Exception('%s : Input config error, it does not have probMat' % os.path.name(__file__))
			if 'num_of_structures' not in self.input_config['modeling_parameters'] :
				raise Exception('%s : Input config error, it does not have num_of_structures' % os.path.name(__file__))
			if 'theta' not in self.input_config['modeling_parameters'] :
				raise Exception('%s : Input config error, it does not have theta' % os.path.name(__file__))
			if 'struct_dir' not in self.input_config['modeling_parameters'] :
				raise Exception('%s : Input config error, it does not have struct_dir' % os.path.name(__file__))
			
		if 'output_dir' not in self.input_config :
			raise Exception('%s : Input config error, it does not have output_dir' % os.path.name(__file__))
			
		if 'system' not in self.input_config :
			raise Exception('%s : Input config error, it does not have system ' % os.path.name(__file__))
		else :
			if 'default_core' not in self.input_config['system'] :
				raise Exception('%s : Input config error, it does not have max_core' % os.path.name(__file__))
			if 'max_memMB' not in self.input_config['system'] :
				raise Exception('%s : Input config error, it does not have max_memMB' % os.path.name(__file__))
				
		compute_violation_src = '%s/compute_violation.py' % self.input_config['source_dir']
		struct_dir = self.input_config['modeling_parameters']['struct_dir']
		theta = self.input_config['modeling_parameters']['theta']
		nstruct = self.input_config['modeling_parameters']['num_of_structures']
		violationFile = self.input_config['modeling_parameters']['violation_file']
		
		nCores = self.input_config['system']['default_core']
		memMb = self.input_config['system']['max_memMB']
		genome = self.input_config['input']['genome']
		resolution = self.input_config['input']['resolution']
		
		python_path = subprocess.Popen("which python", shell=True, stdout=subprocess.PIPE).stdout.read().decode().rstrip('\n')		
		args = [python_path,
			compute_violation_src, 
			'--struct_dir', struct_dir, 
			'--theta', theta,
			('--nstruct %i' % nstruct),
			'--violationFile', violationFile,
			'--genome', genome,
			('--resolution %i' % resolution)]
		
		task_label = "compute_violation_flow"
		self.addTask(label=task_label, command=' '.join(args), nCores=nCores, memMb=memMb, retryMax=3, retryWait=2, retryWindow=0, retryMode="all")	
		
if __name__ == "__main__":
	parser = argparse.ArgumentParser(usage=extend_pyflow_docstring(__doc__))
	parser.add_argument('-i', '--input_config', type=str, required=True)
	# parser.add_argument('-c', '--sys_config', type=str, required=True)
	# parser.add_argument('-s', '--struct_dir', type=str, required=True)
	# parser.add_argument('-theta', '--theta', type=str, required=True)
	# parser.add_argument('-n', '--nstruct', type=str, required=True)
	# parser.add_argument('-o', '--output_dir', type=str, required=True)
	# parser.add_argument('--flag', default=False, action="store_true")
	
	add_pyflow_args(parser)
	args = parser.parse_args()

	# report_wf = ReportFlow(
			# args.sys_config,
			# args.struct_dir,
			# args.theta,
			# args.nstruct,
			# args.output_dir
			# )
	compute_violation_wf = ComputeViolationFlow(args.input_config)
	
	sys.exit(compute_violation_wf.run(**default_pyflow_args(args)))