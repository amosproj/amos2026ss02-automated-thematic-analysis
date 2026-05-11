# Summary of work: LLM for AMOSxNIM

responsible: Ole Müermann & Paul Klarer  
ole.mueermann@fau.de paul.linus.klarer@fau.de


##### Terms & Definitions
NHR (Nationales Hochleistungsrechnen): National High-Performance Computing infrastructure in Germany.
HPC (High Performance Computing): Systems and resources for computationally intensive tasks.

---
### Key Resources & Important links

[NHR Account Web Portal](https://doc.nhr.fau.de/hpc-portal/) - Manage your HPC resources

[NHR Documentation](https://doc.nhr.fau.de/) - Official NHR/FAU documentation

Recommendation to everyone: join the HPC coffe lecture.
Past talks and recordings can be found here:
[HPC Cafe](https://hpc.fau.de/teaching/hpc-cafe/)

##### AI & LLM-Specific Resources

[Requesst LLM API KEY (NHR@FAU)](https://hpc.fau.de/request-llm-api-key/) - Experimental service for LLM access.

[General AI chat models hosted @FAU](https://hawki.ai.fau.de/interface) - Central AI resources at FAU.



---
### Documentation of our Work

1. research for fitting open source models
    - model selection .md
    - Alternatives Considered: Local inference vs. cloudbased solutions
    - Trade-offs between model size and computational efficiency


2. research required hardware for hosting (inference, storage)

3. request access to HPC via hpc portal (small cluster)
    - access got granted by Prof. Riehle
    - configure file storage systems on the cluster via ssh to understand setup possibilities on smaller cluster, and to understand setup constraints
    - Limited GPU availability on small cluster, identified storage quota limits

4. request higher ressources cluster access at NHR
    - more performative access needs to be specialy granted by the NHR team


5. after exchange with the FAU HPC team, they suggested that - if we do not need ONE SPECIFIC model - we use their **experimental** API key service.

    more info for LLMs as a service NHR@FAU:
    https://hpc.fau.de/request-llm-api-key/


    > This experimental service is not being publicly promoted. How did you hear about it?

    - pro: no need to manage
    


6. request sent for API key for the experimental models hosted as LLMs as a service



---
### Interesting Past HPC Talks

##### LLMs for dummies (2025-03-11)
https://hpc.fau.de/files/2025/03/2025-03-11_HPCCafe_LLMfuerDummies.pdf

##### AI on HPC in a nutshell (2025-12-18)
- file systems
- cluster info
https://hpc.fau.de/files/2025/12/2025-12-18_AI_on_HPC_in_a_Nutshell.pdf



