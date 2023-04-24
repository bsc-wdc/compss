LMOD_CMD=/apps/modules/LMOD/8.5.13/lmod/lmod/libexec/lmod

export MODULEPATH=/apps/modules/modulefiles/applications:/apps/modules/modulefiles/compilers:/apps/modules/modulefiles/environment:/apps/modules/modulefiles/libraries:/apps/modules/modulefiles/tools

module ()
{
    eval $($LMOD_CMD bash "$@") && eval $(${LMOD_SETTARG_CMD:-:} -s sh)
}

module load python
module load java
module load compss/Trunk
