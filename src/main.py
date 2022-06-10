import os, random, math
import supervisely_lib as sly

my_app = sly.AppService()

task_id = os.environ["TASK_ID"]
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ['modal.state.slyProjectId'])

_SUFFIX = " Percenatge QC"
PERCENT = int(os.environ['modal.state.samplePercent'])

@my_app.callback("do")
@sly.timeit
def do(**kwargs):
    api = sly.Api.from_env()

    src_project = api.project.get_info_by_id(PROJECT_ID)
    if src_project.type != str(sly.ProjectType.IMAGES):
        raise Exception("Project {!r} has type {!r}. App works only with type {!r}"
                        .format(src_project.name, src_project.type, sly.ProjectType.IMAGES))

    src_project_meta_json = api.project.get_meta(src_project.id)
    src_project_meta = sly.ProjectMeta.from_json(src_project_meta_json)

    

    # create destination project
    dst_name = src_project.name if _SUFFIX in src_project.name else src_project.name + _SUFFIX
    dst_project = api.project.create(WORKSPACE_ID, dst_name, description=_SUFFIX, change_name_if_conflict=True)
    sly.logger.info('Destination project is created.',
                    extra={'project_id': dst_project.id, 'project_name': dst_project.name})

    dst_project_meta = src_project_meta
    api.project.update_meta(dst_project.id, dst_project_meta.to_json())

   

    #getLabelers
    labelers=[]
    for ds_info in api.dataset.get_list(src_project.id):
        ds_progress = sly.Progress('Dataset: {!r}'.format(ds_info.name), total_cnt=ds_info.images_count)
        #dst_dataset = api.dataset.create(dst_project.id, ds_info.name)
        img_infos_all = api.image.get_list(ds_info.id)

        for img_infos in sly.batched(img_infos_all):
            img_names, img_ids, img_metas = zip(*((x.name, x.id, x.meta) for x in img_infos))

            ann_infos = api.annotation.download_batch(ds_info.id, img_ids)
            for ann_info in ann_infos:
                ann = sly.Annotation.from_json(ann_info.annotation, src_project_meta)
                data = ann_info.annotation
                try:
                    if data["tags"][0]["labelerLogin"] not in labelers:
                        labelers.append(data["tags"][0]["labelerLogin"])
                except:
                    pass
                try:
                    if data["objects"][0]["labelerLogin"] not in labelers:
                        labelers.append(data["objects"][0]["labelerLogin"])
                except:
                    pass


    labeler_array = [list() for i in labelers]
    labeler_dict=dict(zip(labelers, labeler_array))

    n=0
    for ds_info in api.dataset.get_list(src_project.id):
        ds_progress = sly.Progress('Dataset: {!r}'.format(ds_info.name), total_cnt=ds_info.images_count)
        #dst_dataset = api.dataset.create(dst_project.id, ds_info.name)
        img_infos_all = api.image.get_list(ds_info.id)

        for img_infos in sly.batched(img_infos_all):
            img_names, img_ids, img_metas = zip(*((x.name, x.id, x.meta) for x in img_infos))

            ann_infos = api.annotation.download_batch(ds_info.id, img_ids)
            for ann_info in ann_infos:
                ann = sly.Annotation.from_json(ann_info.annotation, src_project_meta)
                data = ann_info.annotation
                try:
                    if data["tags"][0]["labelerLogin"] in labeler_dict.keys():
                        labeler_dict[data["tags"][0]["labelerLogin"]].append(n)
                except:
                    pass
                try:
                    if data["objects"][0]["labelerLogin"]in labeler_dict.keys():
                        labeler_dict[data["objects"][0]["labelerLogin"]].append(n)
                except:
                    pass
                n+=1

    randomlist=[]
    for l in labelers:
        totalImagespercentage=math.ceil(len(labeler_dict[l])*(PERCENT/100))

        for u in random.sample(list(labeler_dict[l]),totalImagespercentage):
            randomlist.append(u)
    sly.logger.info('Random images number',extra={'randmlistSize': len(randomlist)})


    ran=0
    for dataset in api.dataset.get_list(src_project.id):
        
        datasett=dataset.name
        
        dst_dataset = api.dataset.create(dst_project.id, dataset.name)
        images = api.image.get_list(dataset.id)

        
        
        annotation=[]
       
        for batch in sly.batched(images):
            
            dst_image_names=[]
            dst_image_ids=[]
            image_ids = [image_info.id for image_info in batch]
            image_names = [image_info.name for image_info in batch]

            ann_infos = api.annotation.download_batch(dataset.id, image_ids)

            anns_to_upload = []
            for ann_info in ann_infos:
                ann = sly.Annotation.from_json(ann_info.annotation,src_project_meta )
                data = ann_info.annotation
                if ran in randomlist:
                    dst_image_names.append(ann_info[1])
                    dst_image_ids.append(ann_info[0])
                    ann = sly.Annotation.from_json(data, dst_project_meta)
                    anns_to_upload.append(ann)
                ran+=1
            dst_image_infos = api.image.upload_ids(dst_dataset.id, dst_image_names, dst_image_ids)
            dst_image_ids = [image_info.id for image_info in dst_image_infos]
            api.annotation.upload_anns(dst_image_ids, anns_to_upload)
    api.task.set_output_project(task_id, dst_project.id, dst_project.name)
    my_app.stop()


def main():
    my_app.run(initial_events=[{"command": "do"}])


if __name__ == "__main__":
    sly.main_wrapper("main", main)