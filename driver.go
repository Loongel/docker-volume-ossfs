package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"regexp"
	"sync"
	"os"
	"time"
	"encoding/json"
	"crypto/md5"
	"errors"
	"strconv"
	"os/exec"
	"io/ioutil"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/sirupsen/logrus"
	"github.com/docker/go-plugins-helpers/volume"
)

type VolumeInfo struct {
	path 		string
	name_ref	string
	bucket		*oss.Bucket
}

type ALiOssVolumeDriver struct {
	debug	   bool
	driver	   string
	volumes    map[string]*VolumeInfo
	clients	   map[string]*oss.Client
	mutex      *sync.Mutex
	mount	   string	// The linked mount path. It's different from "ossfsRoot", I don't know why it is needed, maybe I will retain just one of the two vars later.
	statePath  string
}

type VolumeStorage struct {
	CreatedAt	time.Time
	Driver 		string
	Labels		map[string]string
	Mountpoint	string
	Name		string
	Options		map[string]string
	Scope		string	
}

var debugFlg = true
const ossfsRoot = "/var/lib/ossfs/volumes"	// The fact mount path arg for ossfs program in plugin env   

func NewALiOssVolumeDriver(mount string, driver string, debug bool) volume.Driver {
	clients	:= make(map[string]*oss.Client)

	var d = ALiOssVolumeDriver{
		debug:	    debug,
		driver:	    driver,
		volumes:    make(map[string]*VolumeInfo),
		clients:    clients,
		mutex:      &sync.Mutex{},
		mount:      mount,
		statePath:	filepath.Join(mount, "state", "ossfs-state.json"),
	}

	data, err := ioutil.ReadFile(d.statePath)
	if err != nil {
		if os.IsNotExist(err) {
			logrus.WithField("statePath", d.statePath).Debug("no state found")
		} else {
			logrus.WithField("statePath", d.statePath).Debug("state file found but read failed")
			return nil
		}
	} else {
		if err := json.Unmarshal(data, &d.volumes); err != nil {
			logrus.WithField("statePath", d.statePath).Debug("state file unmarshal failed")
			return nil
		}
	}

	return d
}

func (d ALiOssVolumeDriver) Create(req *volume.CreateRequest) error {

	optionsJson,_ :=json.Marshal(req.Options)
	optionsStr :=string(optionsJson)

	name_ref := req.Options["name_ref"]		
	if name_ref == "" {
		var msg = "name_ref can't be nil!"
		fmt.Printf("%c[1;0;31merror: Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
		return errors.New(msg + "\n" + optionsStr)
	}
	
	endpoint := req.Options["endpoint"]
	if endpoint == "" {
		var msg = "endpoint can't be nil!"
		fmt.Printf("%c[1;0;31merror: Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
		return errors.New(msg)
	}
	
	ak := req.Options["ak"]
	if ak == "" {
		var msg = "AccessKey_ID can't be nil!"
		fmt.Printf("%c[1;0;31merror: Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
		return errors.New(msg)
	}

	sk := req.Options["sk"]
	if sk == "" {
		var msg = "AccessKey_Secret can't be nil!"
		fmt.Printf("%c[1;0;31merror: Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
		return errors.New(msg)
	}
	
	bucket := req.Options["bucket"]
	if bucket == "" {
		var msg = "oss's bucket can't be nil"
		fmt.Printf("%c[1;0;31merror: Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
		return errors.New(msg)
	}
		
    path := req.Options["path"]
	if path == "" {
		path = "/"
	}	
	// oss bucket path handle
	path = regexp.MustCompile(`[/\\]+`).ReplaceAllString(path, string(os.PathSeparator))
	fmt.Printf("%c[1;0;31minfo: path_pre_handel Create volume: %s%c[0m\n",0x1B, path, 0x1B)	
	if path != string(os.PathSeparator) {
		if path[0] == os.PathSeparator {
			path = path[1: len(path)]
		}
		if path[len(path)-1] != os.PathSeparator {
			path = path + string(os.PathSeparator)
		}
	}
	fmt.Printf("%c[1;0;31minfo: path_post_handel Create volume: %s%c[0m\n",0x1B, path, 0x1B)

	
	if req.Name == "" {
		var msg = "volume name can't be nil!---2"
		fmt.Printf("%c[1;0;31merror: Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
		return errors.New(msg)
	}
	
	client, err := oss.New(endpoint, ak, sk)
	if err != nil {
		fmt.Printf("%c[1;0;31merror: create oss client fail by oss define \"%s\"!%c[0m\n",0x1B, name_ref, 0x1B)
	}else{
		d.clients[name_ref] = client;
	}

	return d.BuildVolume(req.Name, name_ref, bucket, path, false)
}

func (d ALiOssVolumeDriver) BuildVolume(name string, name_ref string, bucket string, path string, isLoad bool) error{
	
	client, ok := d.clients[name_ref]
	if client == nil || !ok {
		var msg = fmt.Sprintf("oss client of %s not exists!", name_ref)
		fmt.Printf("%c[1;0;31merror: Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
		return errors.New(msg)
	}
	ok, err := client.IsBucketExist(bucket)
	if !ok {
		var msg = fmt.Sprintf("the bucket of %s not exists in client %s!", bucket, name_ref)
		fmt.Printf("%c[1;0;31merror:  Create volume: %s%c[0m\n", 0x1B, msg, 0x1B)
		if err != nil {
			panic(err)
		}
		return errors.New(msg)
	}
	
	bkt, err := client.Bucket(bucket)
	if bkt == nil || err != nil {
		var msg = fmt.Sprintf("the bucket of %s not exists in client %s!", bucket, name_ref)
                fmt.Printf("%c[1;0;31merror:  Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
                panic(err)
                return errors.New(msg)
        }		

	d.mutex.Lock()
	defer d.mutex.Unlock()
	if _, ok := d.volumes[name]; ok {
                return nil
        }
		
	if path != string(os.PathSeparator){
		ok, err = bkt.IsObjectExist(path)
		if !ok {
			err := bkt.PutObject(path, strings.NewReader(""))
			if err != nil {
				var msg = fmt.Sprintf("create path of %s fail in bucket of %s on client of %s!", path, bucket, name_ref)
				fmt.Printf("%c[1;0;31merror:  Create volume: %s%c[0m\n",0x1B, msg, 0x1B)
							panic(err)
							return errors.New(msg)
			}
		}
	}

	af := d.mountpoint(name, false)
	if !IsExist(af) {
		 os.MkdirAll(af, os.ModePerm)
	}
	go func(){
       		tos, _ := ExecuteCmd("docker volume inspect " + name, 1, d.debug);
    	        if !(strings.Contains(tos, "[") && strings.Contains(tos, "]")) {
			return
            	}
		fp := filepath.Join(af, "opts.json")
		f, err := os.Create(fp)
		if err != nil{
     			fmt.Println(err.Error())
		        return
		}
		defer func(){
			f.Close()
			ExecuteCmd("chmod 110 " + fp, 1, d.debug)
		}()
		umx := fmt.Sprintf("\"Options\": {\n            \"bucket\": \"%s\",\n            \"name_ref\": \"%s\",\n            \"path\": \"%s\"\n        }", bucket, name_ref, path)
		otx := strings.Replace(strings.Replace(strings.Replace(tos, "[", "", -1), "]", "", -1), "\"Options\": null", umx, -1)		
    		f.WriteString(otx)
	}()	
	d.volumes[name] = &VolumeInfo{ path: path, bucket: bkt, name_ref: name_ref }
	
	dfm := "Create"
	if isLoad {
		dfm = "Load"
	}
	fmt.Printf("%s the volume of %s point to %s in bucket of %s in client of %s success!\n", dfm, name, path, bucket, name_ref)
	
	d.saveState()
	
	return nil
}

func (d ALiOssVolumeDriver) List() (*volume.ListResponse, error) {
	logrus.Info("Volumes list... ")
	var res = &volume.ListResponse{}

	volumes := make([]*volume.Volume, 0)

	for name, _ := range d.volumes {
		volumes = append(volumes, &volume.Volume{
			Name:       name,
			Mountpoint: d.mountpoint(name, true),
		})
	}
	
	res.Volumes = volumes
	return res, nil
}

func (d ALiOssVolumeDriver) Get(r *volume.GetRequest) (*volume.GetResponse,error) {
	name := r.Name
	m := strings.Index(name, "[");
        n := strings.Index(name, "]");
      	if m != -1 && n != -1 && n > m {
        	name = strings.Trim(name[0: m] + name[n + 1: len(name)], " ")
	}
	logrus.Infof("Get volume: %s", name)
	var res = &volume.GetResponse{}

	if _, ok := d.volumes[name]; ok {
		res.Volume = &volume.Volume{
			Name:       name,
			Mountpoint: d.mountpoint(name, true),
		}
		return res, nil
	}
	return &volume.GetResponse{}, errors.New(name + " not exists")
}

func (d ALiOssVolumeDriver) Remove(r *volume.RemoveRequest) error {
	logrus.Info("Remove volume ", r.Name)
	d.mutex.Lock()
	defer d.mutex.Unlock()

	vi, ok := d.volumes[r.Name]
	if !ok {
		return errors.New(r.Name + " not exists")
	}
	go func(){
		tos, _ := ExecuteCmd(fmt.Sprintf("find %s/*/opts.json | xargs grep -El '\"Driver\": \"%s\" | \"name_ref\": \"%s\"' | wc -l", d.mount, d.driver,vi.name_ref), 1, d.debug)
		reg := regexp.MustCompile(`\D+`)
		ufx := reg.ReplaceAllString(tos, "")
		cnt, err := strconv.ParseInt(ufx, 10, 32)
		if err != nil {
			panic(err)
			return
		}
                ExecuteCmd("rm -rf " + d.mountpoint(r.Name, false), 2, d.debug)
		if cnt > 1 {
			return
		}
		if d.debug {
			fmt.Printf("current is the last volume of the mount %s, now ready to unmount %s!\n", vi.name_ref, vi.name_ref)
		}
                bkn := vi.bucket.BucketName
                pkp := filepath.Join(ossfsRoot, ToMd5(bkn))
                
                tos, _ = ExecuteCmd("mountpoint " + pkp, 3, d.debug)
                if strings.Contains(tos, "is a mountpoint") || strings.Contains(tos, "是一个挂载点") {
                       	_, err := ExecuteCmd("fusermount -u " + pkp, 4, d.debug)
			if err == nil {
				os.RemoveAll(pkp)				
			}else{
				fmt.Printf("%v", err)
			}
                }else{
			 os.RemoveAll(pkp)
		}
		fmt.Printf("volume remove success!")
	}()
	delete(d.volumes, r.Name)
	d.saveState()
	return nil
}

func (d ALiOssVolumeDriver) Path(r *volume.PathRequest) (*volume.PathResponse,error) {
	logrus.Info("Get volume path ", r.Name)

	var res = &volume.PathResponse{}

	if _, ok := d.volumes[r.Name]; ok {
		res.Mountpoint = d.mountpoint(r.Name, true)
		return res, nil
	}
	return &volume.PathResponse{}, errors.New(r.Name + " not exists")
}

func (d ALiOssVolumeDriver) Mount(r *volume.MountRequest) (*volume.MountResponse,error) {
	logrus.Info("Mount volume ", r.Name)
	vi, ok := d.volumes[r.Name]
	if !ok {
		return &volume.MountResponse{},errors.New(r.Name + " not exists")
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()
	bkn := vi.bucket.BucketName
        pkp := filepath.Join(ossfsRoot, ToMd5(bkn))

	tos, _ := ExecuteCmd("mountpoint " + pkp, 1, d.debug);
	if !(strings.Contains(tos, "is a mountpoint") || strings.Contains(tos, "是一个挂载点")){
		if strings.Contains(tos, "is not a mountpoint") || strings.Contains(tos, "不是一个挂载点"){
			os.RemoveAll(pkp)
		}
		cfg := vi.bucket.Client.Config
		os.Setenv("OSSACCESSKEYID", cfg.AccessKeyID)
		os.Setenv("OSSSECRETACCESSKEY", cfg.AccessKeySecret)
		fmt.Printf("bucket: [%s]  ep: [%s]   aki: [%s]  aks: [%s]\n", vi.bucket.BucketName,  cfg.Endpoint, os.Getenv("OSSACCESSKEYID"), os.Getenv("OSSSECRETACCESSKEY"))
		os.MkdirAll(pkp, os.ModePerm)
		_, err := ExecuteCmd(fmt.Sprintf("ossfs %s %s -ourl=%s", bkn, pkp, cfg.Endpoint), 2, d.debug)
		if err != nil {
			return nil, err
		}
	}
	
	aph := filepath.Join(pkp, vi.path)
	if !IsExist(aph) {
		return nil, errors.New("aim path " + aph + " is not exists!")
	}
	
	af := d.mountpoint(r.Name, true)
	tos, _ = ExecuteCmd("ls -l --color=auto " + af, 2, d.debug);
        if strings.Contains(tos, af + " ->") {
		ExecuteCmd("rm -rf " + af, 3, d.debug)
	}else if strings.Contains(tos, "No such file or directory") || strings.Contains(tos, "没有那个文件或目录"){
		os.MkdirAll(d.mount, os.ModePerm)
		os.RemoveAll(af)
	}
        _, err := ExecuteCmd(fmt.Sprintf("ln -s %s %s", aph, af), 4, d.debug)
        if err != nil {
		panic(err)
		return nil, err
        }
	var res = &volume.MountResponse{}
	res.Mountpoint = af
	return res, nil
}

func (d ALiOssVolumeDriver) Unmount(r *volume.UnmountRequest) error {
	logrus.Info("Unmount ", r.Name)
	_, ok := d.volumes[r.Name]
	if !ok {
		return errors.New(r.Name + " not exists")
	}
	return nil	
}

func (d ALiOssVolumeDriver) Capabilities() *volume.CapabilitiesResponse {
	logrus.Info("Capabilities. ")
	return &volume.CapabilitiesResponse{ Capabilities: volume.Capability{Scope: "global"} }
}

func (d ALiOssVolumeDriver)mountpoint(name string, isData bool) string {
	sm := filepath.Join(d.mount, name)
	if isData {
		return filepath.Join(sm, "_data")
	}else{
		return sm
	}
}

func ToMd5(str string) string {
    data := []byte(str)
    has := md5.Sum(data)
    md5str := fmt.Sprintf("%x", has)
    return md5str
}

func IsExist(f string) bool {
    _, err := os.Stat(f)
    return err == nil || os.IsExist(err)
}

func ExecuteCmd(cmd string, index int, debug bool) (string, error){
	if index != -1 && debug {
		fmt.Printf("	excute %d : %s\n", index, cmd)
	}
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
        tos := string(out)
	if index != -1 && debug {
		prev := "               ---> "
	   	fmt.Printf(prev + strings.Replace(tos, "\n", "\n" + prev, -1) + "\n")
	}
	return tos, err
}

func (d ALiOssVolumeDriver) saveState() {
	data, err := json.Marshal(d.volumes)
	if err != nil {
		logrus.WithField("statePath", d.statePath).Error(err)
		return
	}

	if err := ioutil.WriteFile(d.statePath, data, 0644); err != nil {
		logrus.WithField("savestate", d.statePath).Error(err)
	}
}