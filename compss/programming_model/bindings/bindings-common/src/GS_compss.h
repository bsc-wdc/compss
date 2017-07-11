#ifndef GS_COMPSS_H
#define GS_COMPSS_H

/*** ==============> API FUNCTIONS <================= ***/
extern "C" void GS_On(void);
extern "C" void GS_Off(void);
extern "C" void GS_ExecuteTask(long appId, char *class_name, char *method_name, int priority, int has_target, int num_params, void **params);
extern "C" void GS_ExecuteTaskNew(long appId, char *signature, int priority, int num_nodes, int replicated, int distributed, int has_target, int num_params, void **params);
extern "C" void GS_Get_File(char *file_name, int mode, char **buf);
extern "C" void GS_Close_File(char *file_name, int mode);
extern "C" void GS_Delete_File(char *file_name, int **buf);
extern "C" void GS_Barrier(long appId);
extern "C" void GS_Get_AppDir(char **buf);
//extern "C" void GS_RegisterCE(long appId, char *class_name, char *method_name, int priority, int has_target, int num_params, void **params, char *constraints);
extern "C" void GS_RegisterCE(char *CESignature, char *ImplSignature, char *ImplConstraints, char *ImplType, int num_params, char **ImplTypeArgs);
extern "C" void GS_EmitEvent(int type, long id);

#endif /* GS_COMPSS_H */
