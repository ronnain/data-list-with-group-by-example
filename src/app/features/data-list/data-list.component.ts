import { Component, inject } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import { DataItem, DataListService } from './data-list.service';
import { CommonModule } from '@angular/common';
import {
  BehaviorSubject,
  groupBy,
  map,
  merge,
  mergeMap,
  Observable,
  scan,
  share,
  Subject,
  switchMap,
  tap,
} from 'rxjs';
import {
  SatedStreamStatus,
  statedStream,
} from '../../util/stated-stream/stated-stream';

type StatedVm = {
  isLoading: boolean;
  isLoaded: boolean;
  hasError: boolean;
  error: undefined;
  result: {
    entity: DataItem;
    status: Partial<Record<'update' | 'delete', SatedStreamStatus>>;
  }[];
};
type Pagination = {
  page: number;
  pageSize: number;
};

@Component({
  selector: 'app-data-list',
  templateUrl: './data-list.component.html',
  styleUrls: ['./data-list.component.css'],
  imports: [CommonModule],
})
export class DataListComponent {
  private dataListService = inject(DataListService);

  updateItem$ = new Subject<DataItem>();
  deleteItem$ = new Subject<DataItem>();
  pagination$ = new BehaviorSubject<Pagination>({
    page: 1,
    pageSize: 3,
  });

  private readonly updatingItem$ = this.updateItem$.pipe(
    // takeUntilDestroyed here will avoid to cancel the api call when using the pagination thanks to the constructor subscription, but it will avoid memory leak when navigating on another page
    takeUntilDestroyed(),
    groupBy((updateItem) => updateItem.id),
    mergeMap((group$) => {
      return group$.pipe(
        // switchMap: each time group$ emits, the previous api call will be canceled and the new one will be called (it can be changed using exhaustMap or concatMap). But avoid mergeMap here because it will mix responses
        switchMap((updateItem) =>
          statedStream(this.dataListService.updateItem(updateItem), updateItem)
        )
      );
    }),

    share() // enable multiple subscriptions to the same stream
  );

  private readonly deletingItem$ = this.deleteItem$.pipe(
    // takeUntilDestroyed here will avoid to cancel the api call when using the pagination thanks to the constructor subscription, but it will avoid memory leak when navigating on another page
    takeUntilDestroyed(),
    groupBy((removeItem) => removeItem.id),
    mergeMap((group$) => {
      return group$.pipe(
        switchMap((removeItem) =>
          statedStream(
            this.dataListService.deleteItem(removeItem.id),
            removeItem
          )
        )
      );
    }),
    share() // enable multiple subscriptions to the same stream
  );

  protected readonly vm$: Observable<StatedVm> = this.pagination$.pipe(
    // each time the pagination change, we will call the api to get the data list
    switchMap((pagination) =>
      // merge, we listen to all the streams and add a type property to identify which stream emit
      merge(
        statedStream(this.dataListService.getDataList$(pagination), []).pipe(
          map((dataList) => ({ dataList, type: 'dataList' as const }))
        ),
        this.updatingItem$.pipe(
          map((updatedItem) => ({
            updatedItem,
            type: 'update' as const,
          }))
        ),
        this.deletingItem$.pipe(
          map((deletingItem) => ({
            deletingItem,
            type: 'delete' as const,
          }))
        )
      )
    ),
    // scan it used to accumulate the data and return the new state. (It saves the last emitted state and we can modify it using the "acc" variable)
    scan(
      (acc, curr) => {
        if (curr.type === 'dataList') {
          return {
            ...curr.dataList,
            result: curr.dataList.result.map((entity) => ({
              entity,
              status: {},
            })),
          } satisfies StatedVm;
        }

        if (curr.type === 'update') {
          return {
            ...acc,
            result: acc.result.map((entityData) => {
              if (entityData.entity.id === curr.updatedItem.result.id) {
                return {
                  entity: curr.updatedItem.result,
                  status: {
                    update: {
                      isLoading: curr.updatedItem.isLoading,
                      isLoaded: curr.updatedItem.isLoaded,
                      hasError: curr.updatedItem.hasError,
                      error: curr.updatedItem.error,
                    },
                  } satisfies Partial<
                    Record<'update' | 'delete', SatedStreamStatus>
                  >,
                };
              }
              return entityData;
            }),
          } satisfies StatedVm;
        }

        if (curr.type === 'delete') {
          if (curr.deletingItem.isLoaded) {
            // remove the deleted item from the list
            return {
              ...acc,
              result: acc.result.filter(
                (entityData) =>
                  entityData.entity.id !== curr.deletingItem.result.id
              ),
            } satisfies StatedVm;
          }
          return {
            ...acc,
            result: acc.result.map((entityData) => {
              if (entityData.entity.id === curr.deletingItem.result.id) {
                return {
                  entity: curr.deletingItem.result,
                  status: {
                    delete: {
                      isLoading: curr.deletingItem.isLoading,
                      isLoaded: curr.deletingItem.isLoaded,
                      hasError: curr.deletingItem.hasError,
                      error: curr.deletingItem.error,
                    },
                  } satisfies Partial<
                    Record<'update' | 'delete', SatedStreamStatus>
                  >,
                };
              }
              return entityData;
            }),
          } satisfies StatedVm;
        }
        return acc;
      },
      {
        isLoading: true,
        isLoaded: false,
        hasError: false,
        error: undefined,
        result: [] as {
          entity: DataItem;
          status: Partial<Record<'update' | 'delete', SatedStreamStatus>>;
        }[],
      } satisfies StatedVm as StatedVm
    ),
    tap({
      // It's a gift so you can check that there are no memory leaks (go trigger some actions and navigate to another page)
      complete: () => console.log('[vm$] complete'),
      finalize: () => console.log('[vm$] finalize'),
    })
  );

  constructor() {
    // avoid to cancel the api call when using the pagination
    this.updatingItem$
      .pipe(
        // It's a gift so you can check that there are no memory leaks (go trigger some actions and navigate to another page)
        tap({
          complete: () => console.log('[updatingItem] complete'),
          finalize: () => console.log('[updatingItem] finalize'),
        })
      )
      .subscribe();
    this.deletingItem$
      .pipe(
        // It's a gift so you can check that there are no memory leaks (go trigger some actions and navigate to another page)
        tap({
          complete: () => console.log('[deletingItem] complete'),
          finalize: () => console.log('[deletingItem] finalize'),
        })
      )
      .subscribe();
  }

  previousPage() {
    const currentPage = this.pagination$.value.page;
    if (currentPage > 1) {
      this.pagination$.next({
        ...this.pagination$.value,
        page: currentPage - 1,
      });
    }
  }

  nextPage() {
    const currentPage = this.pagination$.value.page;
    this.pagination$.next({
      ...this.pagination$.value,
      page: currentPage + 1,
    });
  }

  updateItem(item: DataItem) {
    this.updateItem$.next({
      ...item,
      name: 'Item ' + Math.floor(Math.random() * (1000 - 100 + 1) + 100),
    });
  }

  deleteItem(item: DataItem) {
    this.deleteItem$.next(item);
  }
}
