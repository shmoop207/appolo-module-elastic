import {Module, module, IModuleParams} from "@appolo/engine";
import {ElasticProvider, IOptions} from "../index";
import {Defaults} from "./src/defaults";

@module()
export class ElasticModule extends Module<IOptions> {

    protected readonly Defaults: Partial<IOptions> = Defaults;

    public static for(options?: IOptions): IModuleParams {
        return {type:ElasticModule,options}
    }

    public get exports() {
        return [{id: this.moduleOptions.id, type: ElasticProvider}];
    }
}
