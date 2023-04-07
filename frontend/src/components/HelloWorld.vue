<template>
    <div style="height: 700px">
        <VueFlow v-model="element"/>
    </div>
</template>

<script>
import {VueFlow} from '@vue-flow/core'
import axios from "axios";

export default {
    name: 'HelloWorld',
    props: {},
    components: {
        VueFlow
    },
    data() {
        return {
            element: [],
        }
    },
    mounted() {
        this.refresh_graph();
    },
    computed: {},
    methods: {
        refresh_graph() {
            axios.get('/client/tasks/export').then((resp) => {
                console.log(resp);
                let new_map = resp.data;

                // delete none existing keys, updating new keys
                let ori_keys = new Set();
                for (const k of this.element) {
                    ori_keys.add(k.id)
                }
                for (let i=0; i < this.element.length; i+=1) {
                    while (!(this.element[i].id in new_map)) {
                        this.element.splice(i, 1);
                    }
                    let kk = new_map[this.element[i].id];
                    delete (kk['position']);
                    Object.assign(this.element[i], kk);
                }
                // adding new elements
                for (const k in new_map) {
                    if(!ori_keys.has(k)){
                        this.element.push(new_map[k])
                    }
                }

            })
        },
    },
    sockets: {
        graph_change_to_web() {
            console.log('graph change');
            this.refresh_graph();
        },

    }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>

</style>
