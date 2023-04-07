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
            ele_map: {},
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

                // delete none existing keys:
                let ori_keys = [];
                for (const k in this.ele_map) {
                    ori_keys.push(k)
                }
                for (const k of ori_keys) {
                    if (!(k in new_map)) {
                        delete (this.ele_map[k])
                    }
                }
                // updating new keys
                for (const k in new_map) {
                    if (k in this.ele_map) {
                        const kk = new_map[k];
                        delete (kk['position']);
                        Object.assign(this.ele_map[k], kk);
                    } else {
                        this.ele_map[k] = new_map[k];
                    }
                }
                // update elements
                this.element = [];
                for (const k in this.ele_map) {
                    this.element.push(this.ele_map[k]);
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
