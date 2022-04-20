Search.setIndex({docnames:["index","source/modules","source/step_pipeline"],envversion:{"sphinx.domains.c":2,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":4,"sphinx.domains.index":1,"sphinx.domains.javascript":2,"sphinx.domains.math":2,"sphinx.domains.python":3,"sphinx.domains.rst":2,"sphinx.domains.std":2,sphinx:56},filenames:["index.rst","source/modules.rst","source/step_pipeline.rst"],objects:{"":[[2,0,0,"-","step_pipeline"]],"step_pipeline.batch":[[2,1,1,"","BatchPipeline"],[2,1,1,"","BatchStep"],[2,1,1,"","BatchStepType"]],"step_pipeline.batch.BatchPipeline":[[2,2,1,"","backend"],[2,3,1,"","cancel_after_n_failures"],[2,3,1,"","default_cpu"],[2,3,1,"","default_image"],[2,3,1,"","default_memory"],[2,3,1,"","default_python_image"],[2,3,1,"","default_storage"],[2,3,1,"","default_timeout"],[2,3,1,"","gcloud_project"],[2,3,1,"","new_step"],[2,3,1,"","run"]],"step_pipeline.batch.BatchStep":[[2,3,1,"","always_run"],[2,3,1,"","cpu"],[2,3,1,"","memory"],[2,3,1,"","storage"],[2,3,1,"","timeout"]],"step_pipeline.batch.BatchStepType":[[2,4,1,"","BASH"],[2,4,1,"","PYTHON"]],"step_pipeline.constants":[[2,1,1,"","Backend"]],"step_pipeline.constants.Backend":[[2,4,1,"","CROMWELL"],[2,4,1,"","HAIL_BATCH_LOCAL"],[2,4,1,"","HAIL_BATCH_SERVICE"],[2,4,1,"","TERRA"]],"step_pipeline.io":[[2,1,1,"","Delocalize"],[2,1,1,"","InputSpec"],[2,1,1,"","InputSpecBase"],[2,1,1,"","InputType"],[2,1,1,"","InputValueSpec"],[2,1,1,"","Localize"],[2,1,1,"","OutputSpec"]],"step_pipeline.io.Delocalize":[[2,4,1,"","COPY"],[2,4,1,"","GSUTIL_COPY"],[2,4,1,"","HAIL_HADOOP_COPY"]],"step_pipeline.io.InputSpec":[[2,2,1,"","filename"],[2,2,1,"","local_dir"],[2,2,1,"","local_path"],[2,2,1,"","localize_by"],[2,2,1,"","source_bucket"],[2,2,1,"","source_dir"],[2,2,1,"","source_path"],[2,2,1,"","source_path_without_protocol"]],"step_pipeline.io.InputSpecBase":[[2,2,1,"","name"],[2,2,1,"","uuid"]],"step_pipeline.io.InputType":[[2,4,1,"","BOOL"],[2,4,1,"","FLOAT"],[2,4,1,"","INT"],[2,4,1,"","STRING"]],"step_pipeline.io.InputValueSpec":[[2,2,1,"","input_type"],[2,2,1,"","value"]],"step_pipeline.io.Localize":[[2,4,1,"","COPY"],[2,4,1,"","GSUTIL_COPY"],[2,4,1,"","HAIL_BATCH_GCSFUSE"],[2,4,1,"","HAIL_BATCH_GCSFUSE_VIA_TEMP_BUCKET"],[2,4,1,"","HAIL_HADOOP_COPY"],[2,3,1,"","get_subdir_name"]],"step_pipeline.io.OutputSpec":[[2,2,1,"","delocalize_by"],[2,2,1,"","filename"],[2,2,1,"","local_dir"],[2,2,1,"","local_path"],[2,2,1,"","name"],[2,2,1,"","output_dir"],[2,2,1,"","output_path"]],"step_pipeline.main":[[2,5,1,"","pipeline"]],"step_pipeline.pipeline":[[2,1,1,"","Pipeline"],[2,1,1,"","Step"]],"step_pipeline.pipeline.Pipeline":[[2,3,1,"","cancel_after_n_failures"],[2,3,1,"","check_input_glob"],[2,3,1,"","default_cpu"],[2,3,1,"","default_image"],[2,3,1,"","default_memory"],[2,3,1,"","default_output_dir"],[2,3,1,"","default_python_image"],[2,3,1,"","default_storage"],[2,3,1,"","default_timeout"],[2,3,1,"","export_pipeline_graph"],[2,3,1,"","gcloud_project"],[2,3,1,"","get_config_arg_parser"],[2,3,1,"","new_step"],[2,3,1,"","parse_args"],[2,3,1,"","run"]],"step_pipeline.pipeline.Step":[[2,3,1,"","command"],[2,3,1,"","depends_on"],[2,3,1,"","has_upstream_steps"],[2,3,1,"","input"],[2,3,1,"","input_glob"],[2,3,1,"","input_value"],[2,3,1,"","inputs"],[2,3,1,"","name"],[2,3,1,"","output"],[2,3,1,"","output_dir"],[2,3,1,"","outputs"],[2,3,1,"","post_to_slack"],[2,3,1,"","record_memory_cpu_and_disk_usage"],[2,3,1,"","switch_gcloud_auth_to_user_account"],[2,3,1,"","use_previous_step_outputs_as_inputs"],[2,3,1,"","use_the_same_inputs_as"]],"step_pipeline.utils":[[2,6,1,"","GoogleStorageException"],[2,5,1,"","are_any_inputs_missing"],[2,5,1,"","are_outputs_up_to_date"],[2,5,1,"","check_gcloud_storage_region"]],"step_pipeline.wdl":[[2,1,1,"","WdlPipeline"],[2,1,1,"","WdlStep"]],"step_pipeline.wdl.WdlPipeline":[[2,2,1,"","backend"],[2,3,1,"","new_step"],[2,3,1,"","run"],[2,3,1,"","run_for_each_row"]],"step_pipeline.wdl.WdlStep":[[2,3,1,"","cpu"],[2,3,1,"","memory"],[2,3,1,"","storage"]],step_pipeline:[[2,0,0,"-","batch"],[2,0,0,"-","constants"],[2,0,0,"-","io"],[2,0,0,"-","main"],[2,0,0,"-","pipeline"],[2,0,0,"-","utils"],[2,0,0,"-","wdl"]]},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","property","Python property"],"3":["py","method","Python method"],"4":["py","attribute","Python attribute"],"5":["py","function","Python function"],"6":["py","exception","Python exception"]},objtypes:{"0":"py:module","1":"py:class","2":"py:property","3":"py:method","4":"py:attribute","5":"py:function","6":"py:exception"},terms:{"0":2,"01":2,"1":2,"10":2,"12":2,"16":[],"2":2,"20":2,"2020":2,"23":[],"2784":2,"3":2,"4":2,"5":2,"52":2,"6":[],"64":2,"7":2,"abstract":2,"boolean":2,"byte":2,"case":[],"class":2,"default":2,"enum":2,"export":2,"float":2,"function":2,"int":2,"new":2,"public":2,"return":2,"short":2,"switch":2,"true":2,"while":2,A:2,By:[],For:2,If:2,It:2,The:2,_batchpipelin:[],_batchstep:[],_inputspec:[],_inputvaluespec:[],_outputspec:[],_pipelin:[],_step:[],_transfer_all_step:2,_transfer_step:[],abc:2,about:2,accept:2,access:2,accessdeni:2,accommod:[],account:2,accumul:[],across:[],action:[],activ:2,ad:2,add:2,add_argu:2,add_force_command_line_arg:2,add_skip_command_line_arg:2,addit:2,after:2,alia:2,all:2,alloc:2,allow:[],alreadi:2,also:2,altern:2,alwai:2,always_run:2,amount:2,an:2,ani:2,anoth:2,api:2,append:[],appli:2,applic:2,approach:2,approxim:2,ar:2,are_any_inputs_miss:2,are_outputs_up_to_d:2,arg:2,arg_suffix:2,argpars:2,argument:2,argumentpars:2,aspect:2,auth:2,automat:2,avail:2,avoid:2,backend:2,background:2,bai:2,bam:2,base:2,bash:2,batch:1,batchpipelin:2,batchstep:2,batchsteptyp:2,been:[],befor:2,begin:2,being:2,below:[],besid:[],between:2,bgzip:[],bill:2,bool:2,both:2,bp:[],broadinstitut:2,bucket:2,built:2,bw2:2,c:[],cach:2,call:2,can:2,cancel:2,cancel_after_n_failur:2,cannot:2,cat:[],central1:2,chang:2,channel:2,charg:2,check:2,check_gcloud_storage_region:2,check_input_glob:2,chrom:[],cloud:2,cluster:2,code:2,collect:[],com:2,command:2,commmand:[],complet:2,comput:2,concret:[],config:2,config_arg_pars:2,config_file_path:2,configargpars:2,consist:2,constant:1,constructor:2,contain:2,content:1,control:2,copi:2,core:2,correspond:2,count:[],counting_job:[],counts_:[],cp:2,cpu:2,cram:2,creat:2,create_new_job:[],credenti:2,cromwel:2,d:2,dag:2,data:2,debug:2,decid:[],default_cpu:2,default_imag:2,default_memori:2,default_output_dir:2,default_python_imag:2,default_storag:2,default_timeout:2,defin:2,definit:2,delet:2,deloc:2,delocalize_bi:2,depend:2,depends_on:2,desc:[],describ:2,design:[],desir:2,destin:2,detail:2,diagram:2,dict:2,dictionari:[],differ:2,dill:2,dir:2,directori:2,disk:2,doc:[],docker:2,doe:[],doesn:[],don:2,download:2,downstream:2,drive:2,drop:2,dsub:[],durat:2,each:2,easier:2,edt:2,eg:2,egress:2,either:2,encapsul:[],encount:2,end:[],engin:2,entri:2,enumer:[],environ:2,error:2,etc:2,even:2,eventu:[],everi:[],exampl:2,except:2,execut:2,execution_context:[],exist:2,expect:2,expected_region:2,explicitli:2,export_graph:2,export_json:2,export_pipeline_graph:2,express:2,extend:2,extens:2,extra:2,f:[],face:[],fail:2,failur:2,fals:2,far:2,fast:2,featur:2,few:2,file:2,filenam:2,first:2,flag:[],folder:2,forc:2,form:2,fraction:2,frequent:2,from:2,full:2,g:2,gatewai:2,gather:[],gc:2,gcloud:2,gcloud_credentials_path:2,gcloud_project:2,gcloud_user_account:2,gcsfuse:2,gener:2,get_config_arg_pars:2,get_local_path:[],get_output_fil:[],get_subdir_nam:2,gi:2,gib:2,github:2,given:2,glanc:2,glob:2,glob_path:2,googl:2,googlestorageexcept:2,grant:2,graph:2,grep:[],gs:2,gs_path:2,gsutil:2,gsutil_copi:2,gz:[],ha:2,hadoop:2,hail:2,hail_batch_gcsfus:2,hail_batch_gcsfuse_via_temp_bucket:2,hail_batch_loc:2,hail_batch_servic:2,hail_hadoop_copi:2,hailgenet:2,happen:2,has_upstream_step:2,have:2,hb:2,hbl:2,help:2,here:[],highmem:2,how:2,howev:[],http:2,ie:2,ignor:2,ignore_access_denied_except:2,imag:2,image_repositori:2,implement:2,includ:2,index:0,inexpens:2,info:[],initi:2,input:2,input_fil:[],input_file_glob:[],input_files_from:[],input_glob:2,input_typ:2,input_valu:2,inputspec:2,inputspecbas:2,inputtyp:2,inputvaluespec:2,insid:2,instal:2,install_gl:2,interfac:[],intern:[],interpret:2,interv:2,io:1,issu:2,job:2,json:2,k:2,keyword:2,ki:2,kill:2,kwarg:2,l:[],label:[],larg:2,later:2,librari:2,like:2,line:2,list:2,list_glob:[],local:2,local_copi:2,local_dir:2,local_path:2,localbackend:2,localization_root_dir:2,localize_bi:2,localize_inputs_using_gcsfus:[],locat:2,loci:2,log:2,login:2,look:[],lowmem:2,m:2,machin:2,mai:2,main:1,major_vers:2,make:2,mark:2,matter:2,maximum:2,mean:2,memori:2,merg:[],merge_vcf:[],messag:2,metadata:2,method:2,mi:2,minimum:2,minor_vers:2,misc:2,modification_tim:2,modul:[0,1],more:2,mount:2,multipl:2,must:2,my:2,n:2,name:2,nearest:2,need:2,network:2,new_job:[],new_step:2,newer:2,none:2,number:2,numer:2,o:[],object:2,occur:2,older:[],omit:2,onc:2,one:2,onli:2,option:2,org:2,other:2,other_step:2,output:2,output_dir:2,output_fil:[],output_path:2,output_svg_path:2,outputspec:2,own:2,p:2,packag:[0,1],page:0,pai:2,parallel:2,param:[],paramet:2,parent:2,pars:2,parse_arg:2,parse_known_arg:[],part:2,particular:2,pass:2,path:2,per:[],perform:2,persist:2,person:2,pi:2,piec:2,pip:2,pipelin:1,point:2,portion:2,possibl:2,post:2,post_to_slack:2,prefix:2,previous:2,previous_step:2,print:2,privat:2,process:2,produc:2,profil:2,project:2,properti:2,provid:2,push:2,put:2,py:[],python3:2,python:2,python_build_image_nam:2,python_requir:2,r:2,rais:2,random:2,rang:[],re:2,read:2,reason:2,receiv:2,record:2,record_memory_cpu_and_disk_usag:2,region:2,regular:2,rel:2,relat:2,render:2,replac:2,repositori:2,repres:2,request:2,requir:2,resourcefil:2,respons:[],result:2,reus:2,reuse_job:[],reuse_job_from_previous_step:2,root:2,round:2,row:2,rubric:[],run:2,run_for_each_row:2,s1:[],s2:[],s3:[],s3_gather_job:[],s3_job:[],s:2,same:2,sampl:2,scatter:[],search:0,second:2,secret:2,see:2,self:2,seqr:2,serv:2,servic:2,servicebackend:2,set:2,setter:2,sge:[],shell:2,short_nam:[],should:2,silent:2,similar:2,sinc:2,singl:2,situat:2,size:2,size_byt:2,skip:2,slack:2,slack_token:2,slim:2,slower:2,small:2,so:2,some:2,someth:2,sometim:2,sourc:2,source_bucket:2,source_dir:2,source_path:2,source_path_without_protocol:2,sp:2,space:2,spec:2,specif:2,specifi:2,standard:2,stat:2,step1:[],step:2,step_numb:2,step_pipelin:0,storag:2,storageregionexcept:2,store:2,store_tru:[],str:2,string:2,sub:2,subclass:2,subdirectori:2,submit:2,submodul:1,subsequ:2,successfulli:2,suffix:2,summari:[],support:2,sure:2,svg:2,switch_gcloud_auth_to_user_account:2,syntax:2,system:2,t:2,tabix:2,tabl:2,tag:2,task:[],tbi:[],temp:2,temporari:2,terra:2,th:[],than:2,thei:2,thi:2,ti:2,time:2,time_interv:2,timeout:2,togeth:2,toggl:[],token:2,tool:2,transfer:2,travers:2,troubleshoot:2,tsv:[],tupl:2,type:2,typic:2,u:2,under:2,unit:2,unless:2,up:2,updat:2,upstream:2,upstream_step:2,us:2,usag:2,use_gcsfus:[],use_previous_step_outputs_as_input:2,use_the_same_inputs_a:2,user:2,util:1,uuid:2,valid:2,valu:2,variabl:2,variant:[],vcf:2,verbos:2,veri:2,wai:2,wait:2,wc:[],wdl:1,wdlpipelin:2,wdlstep:2,wed:2,weisburd:2,well:2,whatev:2,when:2,where:2,whether:2,which:2,wildcard:2,within:2,without:2,work:2,workaround:2,would:2,wrapper:[],write:2,written:2,yet:[],you:2,your:2},titles:["Welcome to step-pipeline\u2019s documentation!","step_pipeline","step_pipeline package"],titleterms:{batch:2,constant:2,content:[0,2],document:0,indic:0,io:2,main:2,modul:2,packag:2,pipelin:[0,2],s:0,step:0,step_pipelin:[1,2],submodul:2,tabl:0,util:2,wdl:2,welcom:0}})