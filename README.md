EBI EVA - Common Python Utilities


# Assembly retrieval module
Create a object that will download and organise all the assemblies under onw directory.
The scientific name is provided to place the assembly in a species specific subfolder. 
No checks are performed to see if the assembly and the species match.

```python
from ebi_eva_common_pyutils.assembly import NCBIAssembly

assembly = NCBIAssembly('GCA_000008865.1', 'Escherichia coli O157:H7 str. Sakai', download_destination)
assembly.download_or_construct()
assembly.assembly_fasta_path
```

To only download the report
```python
from ebi_eva_common_pyutils.assembly import NCBIAssembly

assembly = NCBIAssembly('GCA_000008865.1', 'Escherichia coli O157:H7 str. Sakai', download_destination)
assembly.download_assembly_report()
assembly.assembly_report_path
```


# Logging

Central logging is provided by the logger module which takes care of propagating all handler to any newly created logger.
You need to create at least one handler for the logging to happen.

```python
from ebi_eva_common_pyutils.logger import logging_config as log_cfg 
log_cfg.add_stderr_handler()
logger = log_cfg.get_logger(__name__)
logger.info('Information')
```

Or from any class
```python
from ebi_eva_common_pyutils.logger import logging_config as log_cfg 

class Foo(log_cfg.AppLogger):

    def bar(self):
        self.info('Information')

def main():
    log_cfg.add_stderr_handler()
```
 
