
Nebula
======


Nebula is a Mesos driven distributed build system. It uses python based
build files, to declare a task DAG and then begins executing jobs.

It uses Mesos (http://mesos.apache.org/) for resource allocation and
Docker (https://www.docker.com/) for dependency management. There are a few
(planned) build targets:
 - Shell : Allows you to execute command lines in a docker environment
 - Workflow : Execute a Galaxy workflow


Example NebulaFile

```
fasta_files = [
"http://www.uniprot.org/uniprot/Q3B891.fasta",
"http://www.uniprot.org/uniprot/P04637.fasta"
]

curl_docker = Docker("curl_docker")

for i, a in enumerate(fasta_files):
    seq = Shell("fasta_%s" % i, "curl %s > out.seq" % a, inputs=None,
        outputs={'fasta_file' : "out.seq"},
        docker=curl_docker
    )
    ms5sum = Shell("md5_%s" % i, "md5sum ${seq_file} > out.md5",
        inputs={ 'seq_file' : seq['fasta_file'] },
        outputs={'md5sum' : 'out.md5'}, docker=curl_docker
    )

```

This creates two workflow DAGs (one for each fasta file), one step to download
the data, and another to do an MD5sum on the file


Example Command:
```
./bin/nebula examples -w /scratch/nebula/
```

This will run the NebulaFile in examples, and use /scratch/nebula on the worker
nodes to do work in.
