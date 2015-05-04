
Nebula
======

Nebula is a large scale workflow build system. It can be used to generate
libraries of workflow. For example, if you need to run Galaxy on 1000 different
samples, Nebula lets you generate and track the workflow requests, create docker
instances of Galaxy and manage the jobs.



Example Script:

```
from nebula.docstore import FileDocStore
from nebula.docstore.util import sync_doc_dir
from nebula.service import GalaxyService
from nebula.galaxy import GalaxyWorkflow
from nebula.target import Target

```

Create a connection to a doc store
```
doc = FileDocStore(file_path="/path/to/my/docstore")
```

Copy files in a directory into the doc store (note, these files will need
associated .json files to describe metadata, such as the uuid)
```
sync_doc_dir("/my/data/file/dir", doc)
```

Load the Workflow
```
workflow = GalaxyWorkflow(ga_file="examples/simple_galaxy/SimpleWorkflow.ga")
```

Create a workflow task from the target data loaded into the docstore
```
input_file_1 = Target(uuid="c39ded10-6073-11e4-9803-0800200c9a66")
input_file_2 = Target(uuid="26fd12a2-9096-4af2-a989-9e2f1cb692fe")
task = nebula.tasks.GalaxyWorkflowTask("workflow_test",
  workflow,
  inputs={
    'input_file_1' : input_file_1,
    'input_file_2' : input_file_2
  },
  "parameters" : {
    "tail_select" : {
      "lineNum" : 3
    }
  }
)
```


To create a service to take care of the task
```
service = GalaxyService(
  docstore=doc,
  galaxy="bgruening/galaxy-stable:dev",
  tool_data=os.path.abspath("tool_data"),
  tool_dir=os.path.abspath("tools"),
  smp=[
    ["MuSE", 8],
    ["pindel", 8],
    ["muTect", 8],
    ["delly", 4],
    ["gatk_bqsr", 12],
    ["gatk_indel", 12],
    ["bwa_mem", 12],
    ["broad_variant_pipline", 28]
  ]
)
```
